# concurrent-kafka-event-consumer

This project demonstrates how to create a concurrent Apache Kafka consumer.
Using a concurrent Kafka event consumer is not recommended.
Please check other options first - more partitions, better batching, and so on.

This concurrent consumer partitions events by using their keys to pin them to a specific thread in an executor.
So events with the same key will always be handled by the same thread.
This is important for the order in which events are processed.
This is similar to how Apache Kafka works under the hood.

Each executor is responsible for a partition of keys.
The executors have a single thread with a queue that maintains the proper order.

This project is only a prototype and not thoroughly tested!
It only demonstrates the basic idea. Not much is done for error handling.
The implementation provides an "at-least-once" delivery.

The use of "enable.auto.commit=true" is not recommended. It is better to use batch commits.
Keeping your **maxBatchSize** relatively low will ensure regular record commits, but will have a performance impact.

Check this test for an example usage: [ConcurrentKafkaConsumerTest.java](src/test/java/org/zhelev/kafka/ConcurrentKafkaConsumerTest.java)
