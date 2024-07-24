package org.zhelev.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KafkaRecordConsumerException extends RuntimeException {

    public ConsumerRecord getConsumerRecord() {
        return consumerRecord;
    }

    private final ConsumerRecord consumerRecord;

    public KafkaRecordConsumerException(ConsumerRecord consumerRecord) {
        super();
        this.consumerRecord = consumerRecord;
    }

    public KafkaRecordConsumerException(String message, ConsumerRecord consumerRecord) {
        super(message);
        this.consumerRecord = consumerRecord;
    }

    public KafkaRecordConsumerException(Throwable throwable, ConsumerRecord consumerRecord) {
        super(throwable);
        this.consumerRecord = consumerRecord;
    }

}
