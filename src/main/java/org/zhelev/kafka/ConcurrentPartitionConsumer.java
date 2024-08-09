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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ConcurrentPartitionConsumer<K, V> implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentPartitionConsumer.class);

    public static final int THREADS_PER_EXECUTOR = 1; // must be 1 to keep record processing order!

    private final KeyPartitionedExecutors keyPartitionedExecutors;

    private final ConcurrentPartitionConsumerConfig<K, V> concurrentPartitionConsumerConfig;

    private final TopicPartition topicPartition;

    // may be use ThreadLocal or scoped variables to handle errors
    private Thread.UncaughtExceptionHandler DEFAULT_EXCEPTION_HANDLER = (t, e) -> {
        throw new RuntimeException("Error in Thread: " + t.getName(), e);
    };

    public ConcurrentPartitionConsumer(final TopicPartition partition,
                                       final ConcurrentPartitionConsumerConfig<K, V> concurrentPartitionConsumerConfig
    ) {
        this.topicPartition = partition;
        this.concurrentPartitionConsumerConfig = concurrentPartitionConsumerConfig;

        keyPartitionedExecutors = new KeyPartitionedExecutors(
                this.concurrentPartitionConsumerConfig.getExecutorSize(),
                THREADS_PER_EXECUTOR, this.concurrentPartitionConsumerConfig.getExecutorQueueSize(),
                String.format("%s#%03d", partition.topic(), partition.partition()), DEFAULT_EXCEPTION_HANDLER);
    }

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }

    public static String getPartitionKey(TopicPartition partition) {
        return partition.topic() + "-" + partition.partition();
    }

    public String getPartitionKey() {
        return getPartitionKey(this.topicPartition);
    }

    public List<ConsumerRecord<K, V>> consume(List<ConsumerRecord<K, V>> partitionRecords)
            throws ConcurrentKafkaConsumerException, InterruptedException {
        return handlePartitionRecords(partitionRecords);
    }

    private List<ConsumerRecord<K, V>> handlePartitionRecords(List<ConsumerRecord<K, V>> partitionRecords)
            throws ConcurrentKafkaConsumerException, InterruptedException {

        List<CompletableFuture<ConsumerRecord<K, V>>> futures = new ArrayList<>();
        Vector<ConsumerRecord<K, V>> processedRecords = new Vector<>();
        long count = 0;

        long start = System.nanoTime();

        for (ConsumerRecord<K, V> record : partitionRecords) {

            if (Thread.interrupted()) {
                throw new InterruptedException("Interrupted for: " + record);
            }

            // passing the processedRecords is a side effect, but we want to keep a handle of the processed records
            CompletableFuture<ConsumerRecord<K, V>> future = processRecord(record, processedRecords);
            futures.add(future);

            // use batching => block process, schedule async commit
            // one sub partition (queue) can slow down all others, but the cumulative time should still be similar
            Boolean isAnyExecutorQueueFull = this.keyPartitionedExecutors.isAnyExecutorQueueFull();
            if (futures.size() >= this.concurrentPartitionConsumerConfig.getMaxBatchSize() || isAnyExecutorQueueFull) {
                try {
                    log.info("Waiting for batch to complete, futures size: {}, max queue loads {}",
                            futures.size(), this.keyPartitionedExecutors.getQueueLoads());
                    handleBatch(futures);
                    count += processedRecords.size();
                } catch (ConcurrentKafkaConsumerException e) {
                    log.error(e.getMessage(), e);
                    ConsumerRecord failedRecord = e.getFailedRecord();
                    log.error("Breaking execution! Failed processing record: {}", failedRecord);
                    throw new ConcurrentPartitionConsumerException(e, processedRecords, this.topicPartition);
                }
            }
        }

        handleBatch(futures);
        count += processedRecords.size();
        futures.clear();

        if (log.isInfoEnabled()) {
            if (count > 0) {
                long duration = System.nanoTime() - start;
                double throughput = (double) count * 1_000_000_000 / duration;
                log.info("[{}] Processing {} records took {} ms, throughput is {} [rec/sec]:", getPartitionKey(), count, TimeUnit.NANOSECONDS.toMillis(duration), String.format("%.2f", throughput));
            }
        }

        return processedRecords;
    }

    private void handleBatch(List<CompletableFuture<ConsumerRecord<K, V>>> futures)
            throws ConcurrentKafkaConsumerException {

        if (!futures.isEmpty()) {
            List<ConsumerRecord<K, V>> batchRecords = processConsumerRecordsBatch(futures);
            if (log.isTraceEnabled()) {
                log.trace("Processed record batch of size: {}", batchRecords.size());
            }
            futures.clear();
        }
    }

    private List<ConsumerRecord<K, V>> processConsumerRecordsBatch(List<CompletableFuture<ConsumerRecord<K, V>>> futures) {
        return futures.stream().map(CompletableFuture::join).collect(Collectors.toList());
    }

    private CompletableFuture<ConsumerRecord<K, V>> processRecord(ConsumerRecord<K, V> kafkaRecord, Vector<ConsumerRecord<K, V>> processedRecords) {
        ExecutorService recordExecutor = keyPartitionedExecutors.getExecutorForKey(kafkaRecord.key());
        final IConcurrentKafkaConsumer<K, V> recordConsumer = concurrentPartitionConsumerConfig.getRecordConsumer();
        return CompletableFuture.supplyAsync(() -> {
            ConsumerRecord<K, V> record = recordConsumer.consume(kafkaRecord);
            processedRecords.add(record);
            return record;
        }, recordExecutor);
    }

    @Override
    public void close() {
        log.warn("Closing partition handler for topic {} and partition {}", this.topicPartition.topic(), this.topicPartition.partition());
        keyPartitionedExecutors.close();
    }

}
