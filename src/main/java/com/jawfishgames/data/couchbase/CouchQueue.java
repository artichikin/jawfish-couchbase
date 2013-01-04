/*
 * Copyright ©2012 Jawfish Games Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.jawfishgames.data.couchbase;

import net.spy.memcached.PersistTo;
import net.spy.memcached.ReplicateTo;
import net.spy.memcached.internal.OperationFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.protocol.views.*;

/**
 * A durable, concurrent, distributed queue built on top of Couchbase 2.0.
 * 
 * This queue supports append() and next() operations. If there is a single producer and single
 * consumer, next() should always return items in their queued order. If there are multiple
 * producer/consumers, next() will return items only roughly in the order they were queued.
 * 
 * TODO: Add support for full idempotency and timeouts on the items being processed.
 * 
 * TODO: Add more paranoid error handling to all couch calls
 */
public class CouchQueue {
   public static final Logger logger      = LoggerFactory.getLogger(CouchQueue.class);

   public static final String DESIGN      = "CouchQueue";

   private static final int   MAX_RETRIES = 10;

   private String             name;
   private CouchbaseClient    couch;
   private View               itemsView;

   private final String       id_key;
   private final String       head_key;
   private final String       tail_key;

   /**
    * Last known value of the monotonically increasing tail counter
    */
   private long               tailCache;

   private boolean            orphanCheck = true;
   private long               lastOrphanCheck;

   public CouchQueue(CouchbaseClient couch, String queueName) {
      this.name = queueName;
      this.couch = couch;
      this.id_key = queueName + "::counter";
      this.head_key = queueName + "::head";
      this.tail_key = queueName + "::tail";

      // init queue if it doesn't exist
      couch.add(head_key, 0, "0");
      couch.add(tail_key, 0, "0");

      installView();
   }

   /**
    * Javascript map function for a view that returns all items still in the queue
    */
   private String getViewCode() {
      return String.format("function (doc, meta) {  if (meta.id.indexOf(\"%s::item-\") == 0) { emit(meta.id, null); }}", name);
   }

   /**
    * Installs the view for this queue by updating the default design
    */
   private void installView() {
      try {
         itemsView = couch.getView(DESIGN, name);
      } catch (Exception e) {
         logger.error("View {} Not loaded: {} ", name, e.getMessage());
      }
      if (itemsView == null) {
         DesignDocument<?> designDocument = null;
         try {
            designDocument = couch.getDesignDocument(DESIGN);
         } catch (InvalidViewException e) {
            logger.error("Adding design document: {} ", DESIGN);
            designDocument = new DesignDocument<Object>(DESIGN);
         }
         ViewDesign viewDesign = new ViewDesign(name, getViewCode());
         designDocument.getViews().add(viewDesign);
         try {
            couch.asyncCreateDesignDoc(designDocument);
         } catch (Exception e) {
            logger.error(e.getMessage(), e);
         }
         CouchUtils.sleep(500); // give time for subsequent view initializers
         try {
            itemsView = couch.getView(DESIGN, name);
            logger.info("Installed view {}", name);
         } catch (Exception e) {
            logger.error(e.getMessage(), e);
         }
      }
      if (itemsView == null) {
         logger.error("Required design '{}' view '{}' not found. The queue may not function properly if not installed", DESIGN, name);
      }
   }

   public boolean viewInstalled() {
      return itemsView != null;
   }

   /**
    * Get the name of this queue
    */
   public String getName() {
      return name;
   }

   /**
    * Deletes the entire queue from couchbase
    */
   public void delete() {
      clear();
      couch.delete(id_key);
      couch.delete(tail_key);
      couch.delete(head_key);
   }

   /**
    * Delete all the items in the queue
    */
   public void clear() {
      while (next() != null) {}
   }

   /**
    * Adds an item to the queue with the weakest/fastest durability options
    */
   public boolean append(String item) {
      return append(item, PersistTo.ZERO, ReplicateTo.ZERO);
   }

   /**
    * Appends an item to the queue with specified durability options.
    */
   public boolean append(String item, PersistTo persistTo, ReplicateTo replicateTo) {
      // issue a unique id for the item
      long id = couch.incr(id_key, 1, 1);

      // store the item
      final String key = keyForItem(id);

      OperationFuture<Boolean> res = couch.add(key, 0, item, persistTo, replicateTo);
      if (!res.getStatus().isSuccess()) {
         logger.error("Add {} failed: {}", key, res.getStatus());
         return false;
      }

      tailCache = couch.incr(tail_key, 1, 1);

      return true;
   }

   /**
    * Generates the unique document key for a queued item
    */
   private String keyForItem(long id) {
      return String.format("%s::item-%d", name, id);
   }

   /**
    * Remove and return the next item from the queue.
    */
   public String next() {
      synchronized (this) {
         if (orphanCheck || System.currentTimeMillis() > lastOrphanCheck + CouchUtils.ONE_SECOND * 5) {
            String item = checkForOrphans();
            if (item != null) {
               return item;
            }
         }
      }

      long head = getHead();
      if (tailCache <= head) {
         tailCache = getTail();
      }
      if (head < tailCache) {
         long mine = couch.incr(head_key, 1, 1);
         return claimItem(mine);
      }
      return null;
   }

   /**
    * Search for any queued items that have an id less than the head, indicating that they were
    * orphaned from the ordered consumption. This function consumes the first orphan found.
    */
   private String checkForOrphans() {
      if (itemsView != null) {
         CouchLock lock = new CouchLock(couch, name);
         if (lock.acquireLock(60)) {
            try {
               logger.debug("Checking for orphans");
               lastOrphanCheck = System.currentTimeMillis();
               ViewResponse res = couch.query(itemsView, new Query().setStale(Stale.UPDATE_AFTER).setLimit(1));
               for (ViewRow row : res) {
                  final String key = row.getKey();
                  long itemId = Long.parseLong(key.replace(name + "::item-", ""));
                  if (itemId < getHead()) {
                     logger.debug("Taking orphan {}", key);
                     return claimItem(itemId);
                  }
               }
            } finally {
               lock.unlock();
            }
         }
      }
      orphanCheck = false;
      return null;
   }

   /**
    * Takes the item off of the queue and returns it, or null if there was an error.
    */
   private String claimItem(long id) {
      final String key = keyForItem(id);
      // attempt to get the item
      int attempts = 0;
      while (attempts++ < MAX_RETRIES) {
         Object item = couch.get(key);
         if (item != null) {
            synchronized (this) {
               orphanCheck = false;
            }
            logger.trace("Taking item {}", key);
            // delete the item from couchbase and return it
            OperationFuture<Boolean> result = couch.delete(key);
            if (result.getStatus().isSuccess()) {
               return item.toString();
            } else {
               logger.debug("Failed to delete item {}", key);
            }
         } else {
            if (id >= tailCache) {
               // we may have incremented passed the tail
               tailCache = getTail();
               // schedule an orphan check for later
               synchronized (this) {
                  orphanCheck = true;
               }
            } else {
               logger.warn("Expected: {} to exist", key);
            }
         }
         if (attempts > 5) {
            logger.debug("Backing off {} tries so far: {}", key, attempts);
         }
         CouchUtils.sleep(Math.min(1000, (int) Math.pow(2, attempts)));
      }
      logger.debug("Giving up after: " + attempts + " attempts");
      return null;
   }

   /**
    * Get the current tail id
    */
   protected long getTail() {
      Object obj = couch.get(tail_key);
      if (obj != null) {
         return Long.parseLong(obj.toString());
      }
      return 0;
   }

   /**
    * Get the current head id
    */
   protected long getHead() {
      Object obj = couch.get(head_key);
      if (obj != null) {
         return Long.parseLong(obj.toString());
      }
      return 0;
   }

   /**
    * A rough estimate of the size of the queue (does not include orphans)
    */
   public long size() {
      tailCache = getTail();
      long head = getHead();
      if (head < tailCache) {
         return tailCache - head;
      } else {
         return 0;
      }
   }
}
