package org.zhelev.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class ConcurrentKafkaConsumerException extends RuntimeException {

    private final ConsumerRecord failedRecord;

    public <K,V> ConsumerRecord<K,V> getFailedRecord() {
        return failedRecord;
    }

    public <K,V> ConcurrentKafkaConsumerException(ConsumerRecord<K,V> consumerRecord) {
        super();
        this.failedRecord = consumerRecord;
    }

    public <K,V> ConcurrentKafkaConsumerException(String message, ConsumerRecord<K,V> consumerRecord) {
        super(message);
        this.failedRecord = consumerRecord;
    }

    public <K,V> ConcurrentKafkaConsumerException(Throwable throwable, ConsumerRecord<K,V> consumerRecord) {
        super(throwable);
        this.failedRecord = consumerRecord;
    }

}
