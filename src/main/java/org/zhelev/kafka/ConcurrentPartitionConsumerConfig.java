package org.zhelev.kafka;

public class ConcurrentPartitionConsumerConfig<K, V> {

    private Integer executorSize = Runtime.getRuntime().availableProcessors();

    public Integer getExecutorSize() {
        return executorSize;
    }

    public void setExecutorSize(Integer executorSize) {
        this.executorSize = executorSize;
    }

    public Integer getQueueSize() {
        return queueSize;
    }

    public void setQueueSize(Integer queueSize) {
        this.queueSize = queueSize;
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

    private Integer queueSize = 10;
    private Integer maxBatchSize = 50;

    private final IConcurrentKafkaConsumer<K, V> recordConsumer;

    public ConcurrentPartitionConsumerConfig(IConcurrentKafkaConsumer<K, V> recordConsumer) {
        this.recordConsumer = recordConsumer;
    }
}
