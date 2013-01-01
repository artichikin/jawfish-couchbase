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

import net.spy.memcached.internal.OperationFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.client.CouchbaseClient;

/**
 * A convenience class for doing distributed locks using couchbase.
 */
public class CouchLock {

   public static final Logger logger                    = LoggerFactory.getLogger(CouchLock.class);

   /**
    * Common prefix for all lock keys
    */
   public static final String LOCK_PREFIX               = "LOCK:";

   public static final String ANONYMOUS                 = "ANONYMOUS";

   /**
    * Maximum milliseconds to sleep between retry lock attempts
    */
   public static final int    MAX_SLEEP_BETWEEN_RETRIES = 1000;

   public String              name;
   private CouchbaseClient    client;

   public CouchLock(CouchbaseClient client, String keyName) {
      this.client = client;
      this.name = LOCK_PREFIX + keyName;

      // couch keys can't be longer than 250 bytes
      assert (name.length() < 250);
   }

   /**
    * Attempt to acquire the lock, but does not block if it is already locked
    * 
    * @param timeout holds the lock for no more than this many seconds.
    * 
    * @return true if the lock was acquired
    */
   public boolean acquireLock(int timeoutInSeconds) {
      return acquireLock(timeoutInSeconds, ANONYMOUS);
   }

   /**
    * Blocks until the lock is acquired
    * 
    * @param timeout holds the lock for no more than this many seconds
    */
   public void lock(int timeoutInSeconds) {
      lock(timeoutInSeconds, ANONYMOUS);
   }

   /**
    * Attempt to acquire the lock, but does not block if it is already locked
    * 
    * @param timeout holds the lock for no more than this many seconds.
    * @param lockHolder optional value for the lock to designate the lock holder (useful for
    *           debugging)
    * 
    * @return true if the lock was acquired
    */
   public boolean acquireLock(int timeoutInSeconds, String lockHolder) {
      logger.debug("Attempting to acquire lock {}", name);
      OperationFuture<Boolean> res = client.add(name, timeoutInSeconds, lockHolder);
      try {
         return res.get();
      } catch (Exception e) {
         logger.error(e.getMessage(), e);
         return false;
      }
   }

   /**
    * Blocks until the lock is acquired
    * 
    * @param timeout holds the lock for no more than this many seconds
    * @param lockHolder optional value for the lock to designate the lock holder (useful for
    *           debugging)
    */
   public void lock(int timeoutInSeconds, String lockHolder) {
      long t0 = CouchUtils.t0();
      int attempts = 0;

      while (true) {
         if (acquireLock(timeoutInSeconds, lockHolder)) {
            break;
         } else {
            attempts++;
            logger.debug("Lock {} not acquired. Trying again...", name);
            CouchUtils.sleep(Math.min(attempts * attempts * 10, MAX_SLEEP_BETWEEN_RETRIES));
         }
      }
      logger.debug("Acquired {} after {} ms", name, CouchUtils.elapsedMillis(t0));
   }

   /**
    * Releases the lock
    * 
    * @return true if the lock released successfully
    */
   public boolean unlock() {
      logger.debug("Released Lock {}", name);
      return client.delete(name).getStatus().isSuccess();
   }

   public String toString() {
      return name;
   }
}
