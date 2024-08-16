package org.zhelev.kafka.partition;

import org.zhelev.kafka.IConcurrentKafkaConsumer;

public class ConcurrentPartitionConsumerConfig<K, V> {

    private Integer executorSize = Runtime.getRuntime().availableProcessors();

    private Integer executorQueueSize = 10;

    private Integer maxBatchSize = 100;

    private Integer maxRetryCount = 10;

    private final IConcurrentKafkaConsumer<K, V> recordConsumer;

    public ConcurrentPartitionConsumerConfig(final IConcurrentKafkaConsumer<K, V> recordConsumer) {
        this.recordConsumer = recordConsumer;
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
