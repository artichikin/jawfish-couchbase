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

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.*;

import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.client.CouchbaseClient;
import com.jawfishgames.data.couchbase.CouchQueue;
import com.jawfishgames.data.couchbase.CouchUtils;

/**
 * Some unit tests that can test and benchmark against a local couchbase installation
 */
public class CouchQueueTest {

   public static final Logger logger     = LoggerFactory.getLogger(CouchQueueTest.class);

   public static final String QUEUE_NAME = "Q00";

   private CouchbaseClient    client;
   private CouchQueue         queue;

   @Before
   public void create() throws IOException {
      client = CouchUtils.createDefaultCouchbaseConnection();
      queue = new CouchQueue(client, QUEUE_NAME);
      Assert.assertTrue(queue.viewInstalled());
   }

   @After
   public void shutdown() throws IOException {
      CouchUtils.sleep(1000);
      client.shutdown(10, TimeUnit.SECONDS);
   }

   @Test
   public void testQueueBasic() throws IOException {
      queue.delete();
      for (int i = 0; i < 3; i++) {
         queue.append("Item" + i);
      }
      Assert.assertEquals(3, queue.size());
      for (int i = 0; i < 3; i++) {
         String item = queue.next();
         Assert.assertNotNull(item);
         Assert.assertEquals("Item" + i, item);
      }
      Assert.assertEquals(0, queue.size());
      queue.delete();
   }

   @Test
   public void testDelete() throws IOException {
      CouchQueue queue = new CouchQueue(client, QUEUE_NAME);
      logger.info("HEAD = {}", queue.getHead());
      logger.info("TAIL = {}", queue.getTail());
      logger.info("SIZE = {}", queue.size());
      queue.delete();
   }

   ////////////////////////////////////////////////////////////////////////////////////////////

   static final int PRODUCERS          = 5;
   static final int CONSUMERS          = 5;
   static final int ITEMS_PER_PRODUCER = 2000;

   /**
    * Launches 5 producer threads and 5 consumer threads. Each producer adds 2000 items to the
    * queue. Each consumer reads items from the the queue and the unit test passes when every item
    * has been processed.
    */
   @Test
   public void testQueueThreaded() throws IOException {
      final CouchQueue prodQueue = new CouchQueue(client, QUEUE_NAME);
      final AtomicBoolean done = new AtomicBoolean(false);
      final AtomicInteger count = new AtomicInteger(0);
      final List<String> inItems = new ArrayList<String>();
      final Set<String> outItems = new HashSet<String>();

      long t0 = CouchUtils.t0();
      final AtomicLong t1 = new AtomicLong(0);

      // make sure queue is empty
      prodQueue.delete();

      logger.info("HEAD = {}", prodQueue.getHead());
      logger.info("TAIL = {}", prodQueue.getTail());
      logger.info("SIZE = {}", prodQueue.size());

      Assert.assertTrue(prodQueue.size() == 0);

      // launch producer threads and generate all the items
      for (int i = 0; i < PRODUCERS; i++) {
         CouchUtils.invokeInThread("Producer-" + i, new Runnable() {
            public void run() {
               for (int i = 0; i < ITEMS_PER_PRODUCER; i++) {
                  final String item = Thread.currentThread().getName() + "-Item-" + i;
                  prodQueue.append(item);
                  synchronized (inItems) {
                     inItems.add(item);
                  }
                  if (count.incrementAndGet() == CONSUMERS * ITEMS_PER_PRODUCER) {
                     t1.set(CouchUtils.t0());
                     logger.info("Production Complete");
                  }
               }
            }
         });
      }

      // concurrently launch all the consumer threads and start processing items
      for (int i = 0; i < CONSUMERS; i++) {
         CouchUtils.invokeInThread("Consumer-" + i, new Runnable() {
            public void run() {
               final CouchQueue queue = new CouchQueue(client, QUEUE_NAME);
               while (!done.get()) {
                  String item = queue.next();
                  if (item != null) {
                     synchronized (outItems) {
                        if (outItems.contains(item)) {
                           logger.error("Ruh-roh, we already processed this item {}", item);
                        }
                        outItems.add(item);
                     }
                  } else {
                     CouchUtils.sleep(50);
                  }
                  // if outItems contains all items produced, we're finally done
                  if (outItems.size() == PRODUCERS * ITEMS_PER_PRODUCER) {
                     done.set(true);
                  }
               }
            }
         });
      }

      // wait for producers and consumers to finish
      while (!done.get()) {
         CouchUtils.sleep(50);
      }
      long t2 = CouchUtils.t0();
      logger.info("Queued in {} ms", CouchUtils.elapsedMillis(t0, t1.get()));
      logger.info("Processed in {} ms", CouchUtils.elapsedMillis(t0, t2));

      // make sure we have all the items processed
      int notFound = 0;
      synchronized (outItems) {
         for (String item : inItems) {
            if (!outItems.contains(item)) {
               logger.error("Couldn't find: {}", item);
               notFound++;
            }
            //Assert.assertTrue(outItems.contains(item));
         }
      }
      Assert.assertEquals(0, notFound);

      logger.info("HEAD = {}", prodQueue.getHead());
      logger.info("TAIL = {}", prodQueue.getTail());
      logger.info("SIZE = {}", prodQueue.size());
   }

}
