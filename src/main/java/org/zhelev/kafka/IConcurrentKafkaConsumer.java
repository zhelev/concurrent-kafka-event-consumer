package org.zhelev.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * A functional interface that defines a consumer of Kafka records in a concurrent manner.
 *
 * This interface provides a single method, {@code consume}, which takes a Kafka record as input and returns
 * a processed record. The implementation of this method is responsible for consuming the record,
 * processing it concurrently with other records, and returning the result.
 *
 * @param <K> the key type of the Kafka record
 * @param <V> the value type of the Kafka record
 * @see ConcurrentKafkaConsumerException
 */
@FunctionalInterface
public interface IConcurrentKafkaConsumer<K, V> {

    ConsumerRecord<K, V> consume(ConsumerRecord<K, V> record) throws ConcurrentKafkaConsumerException;
}
