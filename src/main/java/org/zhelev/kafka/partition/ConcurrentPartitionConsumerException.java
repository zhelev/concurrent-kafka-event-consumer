package org.zhelev.kafka.partition;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.zhelev.kafka.dlq.IDeadLetterQueue;

import java.util.List;

public class ConcurrentPartitionConsumerException extends RuntimeException {

    private final List<ConsumerRecord> processedRecords;

    private final IDeadLetterQueue deadLetterQueue;

    private final TopicPartition topicPartition;

    public ConcurrentPartitionConsumerException(final String message,
                                                final IDeadLetterQueue deadLetterQueue,
                                                final List<ConsumerRecord> processedRecords,
                                                final TopicPartition topicPartition) {
        super(message);
        this.topicPartition = topicPartition;
        this.deadLetterQueue = deadLetterQueue;
        this.processedRecords = processedRecords;
    }

    public ConcurrentPartitionConsumerException(final Throwable throwable,
                                                final IDeadLetterQueue deadLetterQueue,
                                                final List<ConsumerRecord> processedRecords,
                                                final TopicPartition topicPartition) {
        super(throwable);
        this.topicPartition = topicPartition;
        this.deadLetterQueue = deadLetterQueue;
        this.processedRecords = processedRecords;
    }

    public IDeadLetterQueue getDeadLetterQueue() {
        return deadLetterQueue;
    }

    public List<ConsumerRecord> getProcessedRecords() {
        return processedRecords;
    }

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }
}
