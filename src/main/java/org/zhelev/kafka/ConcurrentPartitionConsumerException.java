package org.zhelev.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Vector;

public class ConcurrentPartitionConsumerException extends ConcurrentKafkaConsumerException {

    private final Vector processedRecords;

    private final TopicPartition topicPartition;

    public ConcurrentPartitionConsumerException(final ConsumerRecord consumerRecord, final Vector<ConsumerRecord> processedRecords, final TopicPartition topicPartition) {
        super(consumerRecord);
        this.processedRecords = processedRecords;
        this.topicPartition = topicPartition;
    }

    public ConcurrentPartitionConsumerException(final String message, final ConsumerRecord consumerRecord, final Vector<ConsumerRecord> processedRecords, final TopicPartition topicPartition) {
        super(message, consumerRecord);
        this.processedRecords = processedRecords;
        this.topicPartition = topicPartition;
    }

    public ConcurrentPartitionConsumerException(final Throwable throwable, final ConsumerRecord consumerRecord, final Vector processedRecords, final TopicPartition topicPartition) {
        super(throwable, consumerRecord);
        this.processedRecords = processedRecords;
        this.topicPartition = topicPartition;
    }

    public ConcurrentPartitionConsumerException(final ConcurrentKafkaConsumerException exception, final Vector processedRecords, final TopicPartition topicPartition) {
        super(exception, exception.getFailedRecord());
        this.processedRecords = processedRecords;
        this.topicPartition = topicPartition;
    }

    public Vector<ConsumerRecord> getProcessedRecords() {
        return processedRecords;
    }

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }
}
