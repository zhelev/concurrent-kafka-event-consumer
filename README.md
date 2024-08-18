# concurrent-kafka-event-consumer

!! WORK IN PROGRESS !!

This project demonstrates how to create a concurrent Apache Kafka consumer.
Using a concurrent Kafka event consumer is not recommended.
Please check other options first - more partitions, better batching, and so on.

This concurrent consumer partitions events by using their keys to pin them to a specific thread in an executor.
So events with the same key will always be handled by the same thread.
This ensures the order of processing of events of a given key is preserved.

This implementation supports a consumer listening to multiple topics and partitions.
Each executor is responsible for a partition of keys which were delivered in the batch.
The executors have a single thread with a queue that maintains the proper order.
Keys will be distributed by hashCode(), which means that keys from different topics can be assigned to the same executor.
Thus, one topic can overwhelm the concurrent consumer. In this case, consider creating a consumer per topic.

This project is only a prototype and not thoroughly tested!
It only demonstrates the basic idea. Not much is done for error handling.
This implementation should provide an "at-least-once" delivery.

There is default dead letter queue implementation which be default applies a retry policy with liner backoff.
When using exponential backoff the consumer will be marked as stale if it takes too long to commit an offset.
Switching to exponential backoff is easy by small code modification.

The use of "enable.auto.commit=true" is not recommended. It is better to use the default batched commits.
Keeping the **maxBatchSize** relatively low will ensure regular record commits, but will have a performance impact.

Check this test for an example usage: [ConcurrentKafkaConsumerTest.java](src/test/java/org/zhelev/kafka/ConcurrentKafkaConsumerTest.java)

Test data was generated in a MongoDB by using the following command:
```javascript
function randomIntFromInterval(min, max) { // min and max included 
    return Math.floor(Math.random() * (max - min + 1) + min);
}

for (var i = 0; i < 10000; i++) {
    var id = randomIntFromInterval(1, 100)
    var data = randomIntFromInterval(1, 300)
    db.getCollection("test").insertOne({ id: ("id" + id), data: data })
}
```
