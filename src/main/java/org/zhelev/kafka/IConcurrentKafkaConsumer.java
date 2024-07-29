package org.zhelev.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface IConcurrentKafkaConsumer<K, V> {
    ConsumerRecord<K, V> consume(ConsumerRecord<K, V> record) throws ConcurrentKafkaConsumerException;
}
