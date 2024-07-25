package org.zhelev.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

public class ConcurrentKafkaConsumer<K, V> {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentKafkaConsumer.class);

    public static final int TOTAL_FUTURES_THRESHOLD = 1000;
    public static final int THREADS_PER_EXECUTOR = 1; // must be 1 to keep record processing order!

    private final Properties consumerProperties;
    private final KeyPartitionedExecutors keyPartitionedExecutors;
    private final Duration pollDuration;
    private final Collection<String> topics;
    private final IConcurrentKafkaConsumer<K, V> recordConsumer;
    private final boolean isAutoCommitEnabled;

    public interface IConcurrentKafkaConsumer<K, V> {
        ConsumerRecord<K, V> consume(ConsumerRecord<K, V> record) throws ConcurrentKafkaConsumerException;

        Thread.UncaughtExceptionHandler exceptionHandler(Thread t, Throwable e);
    }

    public ConcurrentKafkaConsumer(Properties consumerProperties, Collection<String> topics, Duration pollDuration,
                                   Integer executorSize, Integer queueSize, IConcurrentKafkaConsumer<K, V> recordConsumer) {
        this.consumerProperties = consumerProperties;
        this.topics = topics;
        this.pollDuration = pollDuration;
        this.recordConsumer = recordConsumer;
        keyPartitionedExecutors = new KeyPartitionedExecutors(executorSize, THREADS_PER_EXECUTOR, queueSize,
                String.join("|", topics), recordConsumer::exceptionHandler);

        this.isAutoCommitEnabled = this.consumerProperties.getProperty("enable.auto.commit", "false").equals("true");
        if (isAutoCommitEnabled) {
            log.warn("Using 'enable.auto.commit=true' is not recommended when processing records concurrently.");
        }
    }

    public void consume() {
        try (final Consumer<K, V> consumer = new KafkaConsumer<>(consumerProperties)) {
            consumer.subscribe(topics);
            long count = 0;
            while (!Thread.currentThread().isInterrupted()) {

                Vector<CompletableFuture> futures = new Vector<>();
                ConsumerRecords<K, V> records = consumer.poll(pollDuration);
                for (TopicPartition partition : records.partitions()) {

                    List<ConsumerRecord<K, V>> partitionRecords = records.records(partition);
                    for (ConsumerRecord<K, V> record : partitionRecords) {
                        CompletableFuture<Void> future = processRecord(record);
                        futures.add(future);

                        // use batching, block and process, do not overwhelm the backend
                        // do not push too much so if a record fails you will not have to replay multiple records
                        float maxQueueLoad = this.keyPartitionedExecutors.getMaxQueueLoad();
                        if (futures.size() >= TOTAL_FUTURES_THRESHOLD || maxQueueLoad >= 0.75) {
                            try {
                                log.info("Waiting for batch to complete, futures size: {}, max queue load {}", futures.size(), maxQueueLoad);
                                List<ConsumerRecord> processedRecords = handleBatch(partition, futures, partitionRecords, consumer);
                                count += processedRecords.size();
                            } catch (ConcurrentKafkaConsumerException e) {
                                log.error(e.getMessage(), e);
                                ConsumerRecord failedRecord = e.getConsumerRecord();
                                log.error("Failed processing record: {}", failedRecord);
                                Thread.currentThread().interrupt(); //  "at-least-once" delivery
                                throw e;
                            } catch (Exception e) {
                                log.error(e.getMessage(), e);
                                Thread.currentThread().interrupt(); //  "at-least-once" delivery
                                throw e;
                            }
                        }
                    }

                    List<ConsumerRecord> processedRecords = handleBatch(partition, futures, partitionRecords, consumer);
                    count += processedRecords.size();
                    log.info("Processed so far {} ", count);
                }

            }
        }
    }

    private <K, V> List<ConsumerRecord> handleBatch(TopicPartition partition, Vector<CompletableFuture> futures,
                                                    List<ConsumerRecord<K, V>> partitionRecords, Consumer<K, V> consumer) {
        List<ConsumerRecord> processedRecords = new ArrayList<>();
        if (futures.size() > 0) {
            processedRecords = processConsumerRecordsBatch(futures);
            commitPartitionOffset(partition, partitionRecords, consumer);
            log.info("Processed record batch of size: {}", processedRecords.size());
            futures.clear();
        }
        return processedRecords;
    }

    private static <K, V> List<ConsumerRecord> processConsumerRecordsBatch(Vector<CompletableFuture> futures) {
        return futures.stream().map(CompletableFuture<ConsumerRecord<K, V>>::join)
                .collect(Collectors.toList());
    }

    private <K, V> void commitPartitionOffset(TopicPartition partition, List<ConsumerRecord<K, V>> partitionRecords, Consumer<K, V> consumer) {
        // use partitionRecords, the processedRecords are not ordered
        if (!this.isAutoCommitEnabled) {
            long lastOffset = partitionRecords.get(partitionRecords.size() - THREADS_PER_EXECUTOR).offset();
            consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + THREADS_PER_EXECUTOR)));
            log.info("Setting partition: {} to offset: {} ", partition, lastOffset + THREADS_PER_EXECUTOR);
        } else {
            log.trace("Messages will be auto commited");
        }
    }

    private CompletableFuture<Void> processRecord(ConsumerRecord<K, V> kafkaRecord) {
        ExecutorService recordExecutor = keyPartitionedExecutors.getExecutorForKey(kafkaRecord.key());
        return CompletableFuture.runAsync(() -> recordConsumer.consume(kafkaRecord), recordExecutor);
    }

}
