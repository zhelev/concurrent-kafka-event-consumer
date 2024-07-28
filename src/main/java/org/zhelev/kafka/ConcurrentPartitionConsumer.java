package org.zhelev.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
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
    private final Consumer<K, V> consumer;

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

    public ConcurrentPartitionConsumer(Consumer consumer, TopicPartition partition,
                                       ConcurrentPartitionConsumerConfig concurrentPartitionConsumerConfig
                                       ) {
        this.topicPartition = partition;
        this.concurrentPartitionConsumerConfig = concurrentPartitionConsumerConfig;
        this.consumer = consumer;

        keyPartitionedExecutors = new KeyPartitionedExecutors(
                this.concurrentPartitionConsumerConfig.getExecutorSize(),
                THREADS_PER_EXECUTOR, this.concurrentPartitionConsumerConfig.getQueueSize(),
                partition.partition() + "@" + partition.topic(), DEFAULT_EXCEPTION_HANDLER);
    }

    private long handlePartitionRecords(List<ConsumerRecord<K, V>> partitionRecords,
                                         Boolean isAutoCommitEnabled) throws ConcurrentKafkaConsumerException, InterruptedException {
        Vector<CompletableFuture> futures = new Vector<>();
        long count = 0;

        for (ConsumerRecord<K, V> record : partitionRecords) {

            if (Thread.interrupted()) {
                throw new InterruptedException("Interrupted for: " + record);
            }

            CompletableFuture<Void> future = processRecord(record);
            futures.add(future);

            // use batching, block and process, do not overwhelm the backend
            // do not push too much so if a record fails you will not have to replay multiple records
            float maxQueueLoad = this.keyPartitionedExecutors.getMaxQueueLoad();
            if (futures.size() >= this.concurrentPartitionConsumerConfig.getMaxBatchSize() || maxQueueLoad >= 0.75) {
                try {
                    log.info("Waiting for batch to complete, futures size: {}, max queue load {}", futures.size(), maxQueueLoad);
                    List<ConsumerRecord> processedRecords = handleBatch(futures, isAutoCommitEnabled);
                    count += processedRecords.size();
                } catch (ConcurrentKafkaConsumerException e) {
                    log.error(e.getMessage(), e);
                    ConsumerRecord failedRecord = e.getConsumerRecord();
                    log.error("Failed processing record: {}", failedRecord);
                    Thread.currentThread().interrupt(); //  "at-least-once" delivery
                    throw e;
                }
            }
        }

        List<ConsumerRecord> processedRecords = handleBatch(futures, isAutoCommitEnabled);
        count += processedRecords.size();
        log.info("Processed in batch {} ", count);
        return count;
    }

    private <K, V> List<ConsumerRecord> handleBatch(Vector<CompletableFuture> futures,
                                                    Boolean isAutoCommitEnabled) throws ConcurrentKafkaConsumerException {
        List<ConsumerRecord> processedRecords = new ArrayList<>();
        if (futures.size() > 0) {
            processedRecords = processConsumerRecordsBatch(futures);
            commitPartitionOffset(processedRecords, isAutoCommitEnabled);
            log.info("Processed record batch of size: {}", processedRecords.size());
            futures.clear();
        }
        return processedRecords;
    }

    private static <K, V> List<ConsumerRecord> processConsumerRecordsBatch(Vector<CompletableFuture> futures) {
        return futures.stream().map(CompletableFuture<ConsumerRecord<K, V>>::join)
                .collect(Collectors.toList());
    }

    private <K, V> void commitPartitionOffset(List<ConsumerRecord> partitionRecords, Boolean isAutoCommitEnabled) {
        // use partitionRecords, the processedRecords are not ordered
        if (!isAutoCommitEnabled) {
            long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
            this.consumer.commitAsync(
                    Collections.singletonMap(this.topicPartition, new OffsetAndMetadata(lastOffset + 1)),
                    (commit, err) -> {
                        if (err != null) {
                            log.warn("Successfully commited {}, last offset {}", commit, lastOffset);
                        } else {
                            String errMsg = "Error on commit " + commit + ", last offset " + lastOffset;
                            log.error(errMsg);
                            throw new RuntimeException("Error on commit " + commit + ", last offset " + lastOffset, err);
                        }
                    });
            log.info("Setting partition: {} to offset: {} ", this.topicPartition, lastOffset + 1);
        } else {
            log.trace("Messages will be auto commited");
        }
    }

    private CompletableFuture<Void> processRecord(ConsumerRecord<K, V> kafkaRecord) {
        ExecutorService recordExecutor = keyPartitionedExecutors.getExecutorForKey(kafkaRecord.key());
        final IConcurrentKafkaConsumer recordConsumer = concurrentPartitionConsumerConfig.getRecordConsumer();
        return CompletableFuture.runAsync(() -> recordConsumer.consume(kafkaRecord), recordExecutor);
    }

    @Override
    public void close() {
        log.warn("Closing partition handler for topic {} and partition {}", this.topicPartition.topic(), this.topicPartition.partition());
        keyPartitionedExecutors.close();
    }

    public void consume(List<ConsumerRecord<K, V>> partitionRecords,
                         Boolean isAutoCommitEnabled) throws ConcurrentKafkaConsumerException, InterruptedException {
        handlePartitionRecords(partitionRecords,  isAutoCommitEnabled);
    }

}
