package org.zhelev.kafka.partition;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zhelev.kafka.ConcurrentKafkaConsumerException;
import org.zhelev.kafka.IConcurrentKafkaConsumer;
import org.zhelev.kafka.dlq.IDeadLetterQueue;
import org.zhelev.kafka.dlq.InMemoryDeadLetterQueue;
import org.zhelev.kafka.utils.KeyPartitionedExecutors;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class ConcurrentPartitionConsumer<K, V> implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentPartitionConsumer.class);

    public static final int THREADS_PER_EXECUTOR = 1; // must be 1 to keep record processing order!

    private final KeyPartitionedExecutors keyPartitionedExecutors;

    private final ConcurrentPartitionConsumerConfig<K, V> concurrentPartitionConsumerConfig;

    private final TopicPartition topicPartition;

    private final IDeadLetterQueue<K, V> deadLetterQueue;

    private final Thread.UncaughtExceptionHandler DEFAULT_EXCEPTION_HANDLER = (t, e) -> {
        throw new RuntimeException("Error in Thread: " + t.getName(), e);
    };

    public ConcurrentPartitionConsumer(final TopicPartition partition,
                                       final ConcurrentPartitionConsumerConfig<K, V> concurrentPartitionConsumerConfig
    ) {
        this.topicPartition = partition;
        this.concurrentPartitionConsumerConfig = concurrentPartitionConsumerConfig;
        this.deadLetterQueue = concurrentPartitionConsumerConfig.getDeadLetterQueue() != null ? concurrentPartitionConsumerConfig.getDeadLetterQueue() : new InMemoryDeadLetterQueue<>();

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

        long start = System.nanoTime();

        for (ConsumerRecord<K, V> record : partitionRecords) {

            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("Interrupted for: " + record);
            }

            // use batching => block process, schedule async commit
            // one sub partition (queue) can slow down all others, but the cumulative time should still be similar
            Boolean isAnyExecutorQueueFull = this.keyPartitionedExecutors.isAnyExecutorQueueFull();
            if (futures.size() >= this.concurrentPartitionConsumerConfig.getMaxBatchSize() || isAnyExecutorQueueFull) {
                if(log.isDebugEnabled()) {
                    log.debug("[{}] Waiting for batch to complete, futures size: {}, max queue loads {}", getPartitionKey(),
                            futures.size(), this.keyPartitionedExecutors.getQueueLoads());
                }
                processBatch(futures);
            }

            // passing the processedRecords is a side effect, but we want to keep a handle of the processed records
            CompletableFuture<ConsumerRecord<K, V>> future = createRecordFuture(record, processedRecords, false);
            futures.add(future);

        }

        // process remaining records
        processBatch(futures);

        // check if some record processing has failed
        handleDeadLetterQueue(processedRecords, futures);

        if (deadLetterQueue.size() > 0) {
            List<ConsumerRecord> processedRecordsCopy = new ArrayList<>(processedRecords);
            throw new ConcurrentPartitionConsumerException("Could not process records", deadLetterQueue,
                    processedRecordsCopy, getTopicPartition());
        }

        if (log.isInfoEnabled()) {
            int count = processedRecords.size();
            if (count > 0) {
                long duration = System.nanoTime() - start;
                double throughput = (double) count * 1_000_000_000 / duration;
                log.info("[{}] Processing {} records took {} ms, throughput is {} [rec/sec]:", getPartitionKey(),
                        count, TimeUnit.NANOSECONDS.toMillis(duration), String.format("%.2f", throughput));
            }
        }

        return processedRecords;
    }

    private void handleDeadLetterQueue(Vector<ConsumerRecord<K, V>> processedRecords,
                                       List<CompletableFuture<ConsumerRecord<K, V>>> futures) throws InterruptedException {
        int retryCount = 0;
        while (deadLetterQueue.size() > 0 && retryCount < concurrentPartitionConsumerConfig.getMaxRetryCount()) {
            retryCount++;
            long sleep = 3 * retryCount * 1000L;
            log.warn("[{}] Handling dead letter queue because of failed keys => retry: {}/{}, sleeping for {}ms, failed #: {}",
                    getPartitionKey(), retryCount, concurrentPartitionConsumerConfig.getMaxRetryCount(), sleep,
                    deadLetterQueue.size());
            log.warn("[{}] DLQ summary (key=# queued) => {}", getPartitionKey(), deadLetterQueue.summary());
            Thread.sleep(sleep);

            deadLetterQueue.keys().forEach(key -> deadLetterQueue.queue(key).forEach(record -> {

                // use batching => block process, schedule async commit
                // one sub partition (queue) can slow down all others, but the cumulative time should still be similar
                Boolean isAnyExecutorQueueFull = this.keyPartitionedExecutors.isAnyExecutorQueueFull();
                if (futures.size() >= this.concurrentPartitionConsumerConfig.getMaxBatchSize() || isAnyExecutorQueueFull) {
                    if(log.isDebugEnabled()) {
                        log.debug("[{}] Waiting for batch to complete, futures size: {}, max queue loads {}", getPartitionKey(),
                                futures.size(), this.keyPartitionedExecutors.getQueueLoads());
                    }
                    processBatch(futures);
                }

                CompletableFuture<ConsumerRecord<K, V>> future = createRecordFuture(record, processedRecords, true);
                futures.add(future);
            }));

            processBatch(futures);

            int blocked = deadLetterQueue.sizeBlocked();
            if (blocked >= concurrentPartitionConsumerConfig.getMaxBlocked()) {
                log.warn("[{}] Number of blocked keys: {}", getPartitionKey(), blocked);
                throw new ConcurrentPartitionConsumerException("Blocked keys limit reached: " + concurrentPartitionConsumerConfig.getMaxBlocked(),
                        deadLetterQueue, new ArrayList<>(processedRecords), getTopicPartition());
            }
            deadLetterQueue.clearBlocks();
        }

        if (retryCount >= concurrentPartitionConsumerConfig.getMaxRetryCount()) {
            log.error("Exhausted all retry attempts {}", retryCount);
            List<ConsumerRecord> processedRecordsCopy = new ArrayList<>(processedRecords);
            throw new ConcurrentPartitionConsumerException("Exhausted all retry attempts: " + retryCount,
                    deadLetterQueue, processedRecordsCopy, getTopicPartition());
        }
    }

    private void processBatch(List<CompletableFuture<ConsumerRecord<K, V>>> futures) {
        try {
            if (!futures.isEmpty()) {
                try {
                    List<ConsumerRecord<K, V>> batchRecords = joinConsumerRecordsBatch(futures);
                    if (log.isTraceEnabled()) {
                        log.trace("[{}] Processed record batch of size: {}", getPartitionKey(), batchRecords.size());
                    }
                } catch (CompletionException completionException) {
                    log.error(completionException.getMessage(), completionException.getCause());
                    if (completionException.getCause() instanceof
                            ConcurrentKafkaConsumerException concurrentKafkaConsumerException) {
                        deadLetterQueue.enqueue(concurrentKafkaConsumerException.getFailedRecord());
                    } else {
                        throw completionException;
                    }
                }
            }
        } finally {
            futures.clear();
        }
    }

    private void cancelFutures(List<CompletableFuture<ConsumerRecord<K, V>>> futures) {
        futures.forEach(f -> {
            try {
                f.cancel(true);
            } catch (Exception e) {
                log.warn("[{}] Error canceling batch future: {}", getPartitionKey(), e.getMessage());
            }
        });
    }

    private List<ConsumerRecord<K, V>> joinConsumerRecordsBatch(
            List<CompletableFuture<ConsumerRecord<K, V>>> futures) {
        return futures.stream().map(CompletableFuture::join).filter(Objects::nonNull).collect(Collectors.toList());
    }

    private CompletableFuture<ConsumerRecord<K, V>> createRecordFuture(
            final ConsumerRecord<K, V> kafkaRecord, final Vector<ConsumerRecord<K, V>> processedRecords,
            final boolean isDeadLetterRecord) {

        ExecutorService recordExecutor = keyPartitionedExecutors.getExecutorForKey(kafkaRecord.key());
        final IConcurrentKafkaConsumer<K, V> recordConsumer = concurrentPartitionConsumerConfig.getRecordConsumer();

        return CompletableFuture.supplyAsync(() -> {

            if (isDeadLetterRecord) {
                if (deadLetterQueue.isBlocked(kafkaRecord.key())) {
                    if (log.isDebugEnabled()) {
                        log.warn("[{}] Key is blocked {}!!!", getPartitionKey(), kafkaRecord.key());
                    }
                    return null;
                }
            } else {
                if (deadLetterQueue.enqueueIfKeyPresent(kafkaRecord)) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Key {} is in the dead letter queue, adding record to dead letter queue for later processing.",
                                getPartitionKey(), kafkaRecord.key());
                    }
                    return null;
                }
            }

            try {
                ConsumerRecord<K, V> record = recordConsumer.consume(kafkaRecord);
                processedRecords.add(record);

                if (isDeadLetterRecord) {
                    deadLetterQueue.evict(kafkaRecord);
                }
                return record;
            } catch (Throwable throwable) {
                if (!isDeadLetterRecord) {
                    deadLetterQueue.enqueue(kafkaRecord);
                } else {
                    if(log.isDebugEnabled()) {
                        log.debug("[{}] Processing record for key {} failed again, blocking!", getPartitionKey(), kafkaRecord.key());
                    }
                    deadLetterQueue.block(kafkaRecord.key());
                }
            }

            return null;
        }, recordExecutor);

    }

    @Override
    public void close() {
        log.warn("[{}] Closing partition handler for topic {} and partition {}", getPartitionKey(), this.topicPartition.topic(), this.topicPartition.partition());
        keyPartitionedExecutors.close();
    }

}
