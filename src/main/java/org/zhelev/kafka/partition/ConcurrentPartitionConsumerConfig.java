package org.zhelev.kafka.partition;

import org.zhelev.kafka.IConcurrentKafkaConsumer;
import org.zhelev.kafka.dlq.IDeadLetterQueue;
import org.zhelev.kafka.dlq.InMemoryDeadLetterQueue;

public class ConcurrentPartitionConsumerConfig<K, V> {

    private Integer executorSize = Runtime.getRuntime().availableProcessors();

    private Integer executorQueueSize = 10;

    private Integer maxBatchSize = 100;

    private Integer maxRetryCount = 10;

    private Integer maxBlocked = 200;

    private final IConcurrentKafkaConsumer<K, V> recordConsumer;

    private IDeadLetterQueue<K, V> deadLetterQueue;

    public ConcurrentPartitionConsumerConfig(final IConcurrentKafkaConsumer<K, V> recordConsumer) {
        this.recordConsumer = recordConsumer;
        this.deadLetterQueue = new InMemoryDeadLetterQueue<>();
    }

    public ConcurrentPartitionConsumerConfig(final IConcurrentKafkaConsumer<K, V> recordConsumer,
                                             final IDeadLetterQueue<K, V> deadLetterQueue) {
        this.recordConsumer = recordConsumer;
        this.deadLetterQueue = deadLetterQueue;
    }

    public Integer getMaxBlocked() {
        return maxBlocked;
    }

    public void setMaxBlocked(Integer maxBlocked) {
        this.maxBlocked = maxBlocked;
    }


    public IDeadLetterQueue<K, V> getDeadLetterQueue() {
        return deadLetterQueue;
    }

    public void setDeadLetterQueue(IDeadLetterQueue<K, V> deadLetterQueue) {
        this.deadLetterQueue = deadLetterQueue;
    }

    public Integer getMaxRetryCount() {
        return maxRetryCount;
    }

    public void setMaxRetryCount(Integer maxRetryCount) {
        this.maxRetryCount = maxRetryCount;
    }


    public Integer getExecutorSize() {
        return executorSize;
    }

    public void setExecutorSize(Integer executorSize) {
        this.executorSize = executorSize;
    }

    public Integer getExecutorQueueSize() {
        return executorQueueSize;
    }

    public void setExecutorQueueSize(Integer executorQueueSize) {
        this.executorQueueSize = executorQueueSize;
    }

    public Integer getMaxBatchSize() {
        return maxBatchSize;
    }

    public void setMaxBatchSize(Integer maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }

    public IConcurrentKafkaConsumer<K, V> getRecordConsumer() {
        return recordConsumer;
    }
}
