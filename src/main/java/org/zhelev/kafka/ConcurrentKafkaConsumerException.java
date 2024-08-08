package org.zhelev.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class ConcurrentKafkaConsumerException extends RuntimeException {

    private final ConsumerRecord failedRecord;

    public ConsumerRecord getFailedRecord() {
        return failedRecord;
    }

    public ConcurrentKafkaConsumerException(ConsumerRecord consumerRecord) {
        super();
        this.failedRecord = consumerRecord;
    }

    public ConcurrentKafkaConsumerException(String message, ConsumerRecord consumerRecord) {
        super(message);
        this.failedRecord = consumerRecord;
    }

    public ConcurrentKafkaConsumerException(Throwable throwable, ConsumerRecord consumerRecord) {
        super(throwable);
        this.failedRecord = consumerRecord;
    }

}
