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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactory;

/**
 * Various utility methods for working with Couchbase
 */
public class CouchUtils {

   public static final Logger logger     = LoggerFactory.getLogger(CouchUtils.class);

   public static final long   ONE_SECOND = 1000;
   public static final long   ONE_MINUTE = 60 * ONE_SECOND;
   public static final long   ONE_HOUR   = ONE_MINUTE * 60;
   public static final long   ONE_DAY    = ONE_HOUR * 24;
   public static final long   ONE_WEEK   = ONE_DAY * 7;

   /**
    * Creates a connection to a default localhost couchbase connection
    */
   public static CouchbaseClient createDefaultCouchbaseConnection() throws IOException {
      String[] hosts = { "localhost" };
      return createCouchbaseConnection("default", "admin", "", hosts);
   }

   public static CouchbaseClient createCouchbaseConnection(String bucket, String user, String password, String[] hosts) throws IOException {
      //System.setProperty("net.spy.log.LoggerImpl", "net.spy.memcached.compat.log.Log4JLogger");
      List<URI> baseURIs = new ArrayList<URI>();
      try {
         for (String host : hosts) {
            baseURIs.add(new URI(String.format("http://%s:8091/pools", host)));
         }
      } catch (URISyntaxException e) {
         logger.error(e.getMessage(), e);
      }
      CouchbaseConnectionFactory cf = new CouchbaseConnectionFactory(baseURIs, bucket, password);
      return new CouchbaseClient(cf);
   }

   /**
    * Sleep current thread for a time, ignoring interrupted exceptions
    */
   public static void sleep(int millis) {
      try {
         Thread.sleep(millis);
      } catch (InterruptedException e) {/* we don't care */}
   }

   /**
    * Get the elapsed milliseconds between two nano times
    */
   public static long elapsedMillis(long t0nanos, long t1nanos) {
      return (t1nanos - t0nanos) / 1000000;
   }

   /**
    * Get the current time in nanos
    */
   public static long t0() {
      return System.nanoTime();
   }

   /**
    * Get the elapsed milliseconds since the given nano time.
    */
   public static long elapsedMillis(long t0) {
      return elapsedMillis(t0, t0());
   }

   public static void invokeInThread(String threadName, Runnable runnable) {
      Thread t = new Thread(runnable, threadName);
      t.start();
   }

}
