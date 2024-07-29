package org.zhelev.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

public class ConcurrentPartitionConsumer<K, V> implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentKafkaConsumer.class);

    public static final int THREADS_PER_EXECUTOR = 1; // must be 1 to keep record processing order!

    private final KeyPartitionedExecutors keyPartitionedExecutors;
    private final ConcurrentPartitionConsumerConfig concurrentPartitionConsumerConfig;

    private final TopicPartition topicPartition;

    // may be use ThreadLocal or scoped variables to handle errors
    private Thread.UncaughtExceptionHandler DEFAULT_EXCEPTION_HANDLER = (t, e) -> {
        throw new RuntimeException("Error in Thread: " + t.getName(), e);
    };

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }

    public static String getPartitionKey(TopicPartition partition) {
        final String partitionKey = partition.topic() + "-" + partition.partition();
        return partitionKey;
    }

    public String getPartitionKey() {
        return getPartitionKey(this.topicPartition);
    }

    public ConcurrentPartitionConsumer(TopicPartition partition,
                                       ConcurrentPartitionConsumerConfig concurrentPartitionConsumerConfig
    ) {
        this.topicPartition = partition;
        this.concurrentPartitionConsumerConfig = concurrentPartitionConsumerConfig;

        keyPartitionedExecutors = new KeyPartitionedExecutors(
                this.concurrentPartitionConsumerConfig.getExecutorSize(),
                THREADS_PER_EXECUTOR, this.concurrentPartitionConsumerConfig.getQueueSize(),
                String.format("%s#%03d", partition.topic(), partition.partition()), DEFAULT_EXCEPTION_HANDLER);
    }

    private List<ConsumerRecord<K, V>> handlePartitionRecords(List<ConsumerRecord<K, V>> partitionRecords) throws ConcurrentKafkaConsumerException, InterruptedException {
        Vector<CompletableFuture<ConsumerRecord<K, V>>> futures = new Vector<>();
        long count = 0;

        Vector<ConsumerRecord<K, V>> processedRecords = new Vector<>();

        for (ConsumerRecord<K, V> record : partitionRecords) {

            if (Thread.interrupted()) {
                throw new InterruptedException("Interrupted for: " + record);
            }

            CompletableFuture<ConsumerRecord<K, V>> future = processRecord(record);
            futures.add(future);

            // use batching, block and process, do not overwhelm the backend
            float maxQueueLoad = this.keyPartitionedExecutors.getMaxQueueLoad();
            if (futures.size() >= this.concurrentPartitionConsumerConfig.getMaxBatchSize() || maxQueueLoad >= 0.90) {
                try {
                    log.info("Waiting for batch to complete, futures size: {}, max queue load {}", futures.size(), maxQueueLoad);
                    List<ConsumerRecord<K, V>> batchRecords = handleBatch(futures);
                    processedRecords.addAll(batchRecords);
                    count += processedRecords.size();
                } catch (ConcurrentKafkaConsumerException e) {
                    log.error(e.getMessage(), e);
                    ConsumerRecord failedRecord = e.getConsumerRecord();
                    log.error("Failed processing record: {}", failedRecord);
                    throw e;
                }
            }
        }

        List<ConsumerRecord<K, V>> batchRecords = handleBatch(futures);
        processedRecords.addAll(batchRecords);
        count += processedRecords.size();
        if (log.isTraceEnabled()) {
            log.trace("Processed in batch {} ", count);
        }
        futures.clear();
        return processedRecords;
    }

    private List<ConsumerRecord<K, V>> handleBatch(Vector<CompletableFuture<ConsumerRecord<K, V>>> futures) throws ConcurrentKafkaConsumerException {
        List<ConsumerRecord<K, V>> processedRecords = new ArrayList<>();
        if (futures.size() > 0) {
            processedRecords = processConsumerRecordsBatch(futures);
            if (log.isTraceEnabled()) {
                log.trace("Processed record batch of size: {}", processedRecords.size());
            }
            futures.clear();
        }
        return processedRecords;
    }

    private List<ConsumerRecord<K, V>> processConsumerRecordsBatch(Vector<CompletableFuture<ConsumerRecord<K, V>>> futures) {
        return futures.stream().map(CompletableFuture<ConsumerRecord<K, V>>::join)
                .collect(Collectors.toList());
    }

    private CompletableFuture<ConsumerRecord<K, V>> processRecord(ConsumerRecord<K, V> kafkaRecord) {
        ExecutorService recordExecutor = keyPartitionedExecutors.getExecutorForKey(kafkaRecord.key());
        final IConcurrentKafkaConsumer recordConsumer = concurrentPartitionConsumerConfig.getRecordConsumer();
        return CompletableFuture.supplyAsync(() -> recordConsumer.consume(kafkaRecord), recordExecutor);
    }

    @Override
    public void close() {
        log.warn("Closing partition handler for topic {} and partition {}", this.topicPartition.topic(), this.topicPartition.partition());
        keyPartitionedExecutors.close();
    }

    public List<ConsumerRecord<K, V>> consume(List<ConsumerRecord<K, V>> partitionRecords) throws ConcurrentKafkaConsumerException, InterruptedException {
        return handlePartitionRecords(partitionRecords);
    }

}
