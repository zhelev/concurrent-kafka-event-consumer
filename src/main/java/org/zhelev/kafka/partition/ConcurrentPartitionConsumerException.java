package org.zhelev.kafka.partition;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.io.Serializable;
import java.util.Set;

public class ConcurrentPartitionConsumerException extends RuntimeException {

    private final Set<ConsumerRecord> processedRecords;

    private final Set<ConsumerRecord> failedRecords;

    private final TopicPartition topicPartition;

    public ConcurrentPartitionConsumerException(final String message,
                                                final Set<ConsumerRecord> failedRecords,
                                                final Set<ConsumerRecord> processedRecords,
                                                final TopicPartition topicPartition) {
        super(message);
        this.topicPartition = topicPartition;
        this.failedRecords = failedRecords;
        this.processedRecords = processedRecords;
    }

    public ConcurrentPartitionConsumerException(final Throwable throwable,
                                                final Set<ConsumerRecord> failedRecords,
                                                final Set<ConsumerRecord> processedRecords,
                                                final TopicPartition topicPartition) {
        super(throwable);
        this.topicPartition = topicPartition;
        this.failedRecords = failedRecords;
        this.processedRecords = processedRecords;
    }

    public Set<ConsumerRecord> getFailedRecords() {
        return failedRecords;
    }

    public Set<ConsumerRecord> getProcessedRecords() {
        return processedRecords;
    }

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }
}
