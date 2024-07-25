package org.zhelev.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface IConcurrentKafkaConsumer<K, V> {
        void consume(ConsumerRecord<K, V> record) throws ConcurrentKafkaConsumerException;
    }
