package org.zhelev.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class ConcurrentKafkaConsumerException extends RuntimeException {

    public ConsumerRecord getConsumerRecord() {
        return consumerRecord;
    }

    private ConsumerRecord consumerRecord;

    public ConcurrentKafkaConsumerException(ConsumerRecord consumerRecord) {
        super();
        this.consumerRecord = consumerRecord;
    }

    public ConcurrentKafkaConsumerException(String message, ConsumerRecord consumerRecord) {
        super(message);
        this.consumerRecord = consumerRecord;
    }

    public ConcurrentKafkaConsumerException(Throwable throwable, ConsumerRecord consumerRecord) {
        super(throwable);
        this.consumerRecord = consumerRecord;
    }

    public ConcurrentKafkaConsumerException(String message, Throwable e) {
        super(message, e);
    }

}
