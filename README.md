jawfish-couchbase
=================

[Jawfish Games](http://www.jawfishgames.com/) is a Seattle based games company. We specialize in real-time multi-player tournament games for mobile and web. 
We currently use [Couchbase 2.0](http://couchbase.com/) for our NoSQL storage needs. This project contains useful java classes for working with Couchbase 2.0.

### CouchLock
A simple class for using distributed locks on top of Couchbase. Usage is simple:

```java
    CouchLock lock = new CouchLock(couchbaseClient, "MyNamedLock");  
    
    // Blocks until the lock is acquired. The lock will be auto-unlocked after 60 seconds
    lock.lock(60); 
    try {
       // do your dirty business
    } finally {
       lock.unlock();
    }
    
    // you can also do non-blocking attempts to aquire the lock
    if (lock.aquireLock(60)) {
       try {
          // do your dirty business
       } finally {
          lock.unlock();
       }
    }
    
```


### CouchQueue
A durable distributed queue built on top of Couchbase 2.0. If you're already using Couchbase, why add another complex 
dependency to your system for work queues? Just use our CouchQueue implementation! 

```java
    CouchQueue queue = new CouchQueue(couchbaseClient, "MyNamedQueue");
    
    // appends an item to the queue:
    queue.append("My Work Item");
    
    // pops an item off of the queue
    String myWorkItem = queue.next();    
```

**Note: This class is a work-in-progress and still has a few edge cases and features to add before it can be 
considered 100% durable, but this first reference implementation works well in practice.**  

This implementation that should have the following guarantees:

1. If there is a single producer and single consumer, all items will be removed in the order they were queued.
2. If there are multiple consumers, the items will be processing mostly in the order they were queued. We don't guarantee perfect ordering, 
   but the general case is that the oldest items will be consumed before the newest    
3. Consumers won't consume the same item twice (Under normal operating conditions! We still have some work to handle edge cases around 
   couchbase node failures that could lead to the same item being returned by next() more than once). For now you will need to handle idempotency,
   but our goal is to eventually handle that internally.

#### How It Works
A distributed work queue is a very difficult thing to implement on a vanilla NoSQL distributed key-value store. Couchbase 2.0 has two features that
made this implementation tractable. Couchbase has atomic increment support, so we can easily and quickly increment counters without contention between
the producers and consumers. Couchbase 2.0 has _views_ that allow querying of the documents. This allows us to optimize the general case for append()
and next() to have very little contention or coordination, and then use view queries to 'mop up' the edge cases that are otherwise too expensive to 
handle and still be scalable.

#### Performance
TODO: We have not yet benchmarked the raw performance on a large couchbase cluster, but we expect many thousands of items/second can be enqueued and dequeued concurrently.

#### Testing
TODO: Testing under heavy load concurrently with couchbase node failure and consumer node failures.

