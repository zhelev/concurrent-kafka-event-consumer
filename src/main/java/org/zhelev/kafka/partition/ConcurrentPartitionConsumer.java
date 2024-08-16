package org.zhelev.kafka.partition;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zhelev.kafka.ConcurrentKafkaConsumerException;
import org.zhelev.kafka.IConcurrentKafkaConsumer;
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

    private final ConcurrentHashMap<K, Set<ConsumerRecord<K, V>>> deadLetterQueue;

    private final Thread.UncaughtExceptionHandler DEFAULT_EXCEPTION_HANDLER = (t, e) -> {
        throw new RuntimeException("Error in Thread: " + t.getName(), e);
    };

    public ConcurrentPartitionConsumer(final TopicPartition partition,
                                       final ConcurrentPartitionConsumerConfig<K, V> concurrentPartitionConsumerConfig
    ) {
        this.topicPartition = partition;
        this.concurrentPartitionConsumerConfig = concurrentPartitionConsumerConfig;
        this.deadLetterQueue = new ConcurrentHashMap<>();

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

            // passing the processedRecords is a side effect, but we want to keep a handle of the processed records
            CompletableFuture<ConsumerRecord<K, V>> future = createRecordFuture(record, processedRecords, false);
            futures.add(future);

            // use batching => block process, schedule async commit
            // one sub partition (queue) can slow down all others, but the cumulative time should still be similar
            Boolean isAnyExecutorQueueFull = this.keyPartitionedExecutors.isAnyExecutorQueueFull();
            if (futures.size() >= this.concurrentPartitionConsumerConfig.getMaxBatchSize() || isAnyExecutorQueueFull) {
                log.info("[{}] Waiting for batch to complete, futures size: {}, max queue loads {}", getPartitionKey(),
                        futures.size(), this.keyPartitionedExecutors.getQueueLoads());
                processBatch(futures);
            }
        }

        // process remaining records
        processBatch(futures);

        // check if some record processing has failed
        handleDeadLetterQueue(processedRecords, futures);

        if (!deadLetterQueue.isEmpty()) {
            Set<ConsumerRecord> failedRecords = deadLetterQueue.values().stream().flatMap(Set::stream).collect(Collectors.toSet());
            Set<ConsumerRecord> processedRecordsCopy = new HashSet<>(processedRecords);
            throw new ConcurrentPartitionConsumerException("Could not process records", failedRecords, processedRecordsCopy, getTopicPartition());
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

    private void handleDeadLetterQueue(Vector<ConsumerRecord<K, V>> processedRecords, List<CompletableFuture<ConsumerRecord<K, V>>> futures) throws InterruptedException {
        int retryCount = 0;
        while (!deadLetterQueue.isEmpty() || retryCount >= concurrentPartitionConsumerConfig.getMaxRetryCount()) {
            retryCount++;
            long sleep = retryCount * retryCount * 1000L;
            log.warn("[{}] Handling dead letter queue because of failed keys => retry: {}/{}, sleeping for {}ms, failed #: {}",
                    getPartitionKey(), retryCount, concurrentPartitionConsumerConfig.getMaxRetryCount(), sleep,
                    deadLetterQueue.keySet().size());
            Thread.sleep(sleep);

            deadLetterQueue.values().forEach(records -> {
                records.forEach(record -> {
                    CompletableFuture<ConsumerRecord<K, V>> future = createRecordFuture(record, processedRecords, true);
                    futures.add(future);
                });
            });

            processBatch(futures);
        }

        if (retryCount >= concurrentPartitionConsumerConfig.getMaxRetryCount()) {
            log.error("Exhausted all retry attempts {}", retryCount);
        }
    }

    private void processBatch(List<CompletableFuture<ConsumerRecord<K, V>>> futures) {
        try {
            handleBatch(futures);
        } catch (ConcurrentKafkaConsumerException e) {
            cancelFutures(futures);
            ConsumerRecord<K, V> failedRecord = e.getFailedRecord();
            deadLetterQueue.computeIfAbsent(failedRecord.key(), k -> getConcurrentSet());
            deadLetterQueue.get(failedRecord.key()).add(failedRecord);

            // throw new ConcurrentPartitionConsumerException(e, processedRecords, this.topicPartition);
        } catch (Throwable th) {
            // log.error("#####");
            log.error(th.getMessage(), th);
            // throw th;
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

    private void handleBatch(List<CompletableFuture<ConsumerRecord<K, V>>> futures)
            throws ConcurrentKafkaConsumerException {

        if (!futures.isEmpty()) {
            try {
                List<ConsumerRecord<K, V>> batchRecords = processConsumerRecordsBatch(futures);
                if (log.isTraceEnabled()) {
                    log.trace("[{}] Processed record batch of size: {}", getPartitionKey(), batchRecords.size());
                }
            } catch (CompletionException completionException) {
                log.error(completionException.getMessage(), completionException.getCause());
                if (completionException.getCause() instanceof
                        ConcurrentKafkaConsumerException concurrentKafkaConsumerException) {
                    throw concurrentKafkaConsumerException;
                } else {
                    // log.error(completionException.getMessage(), completionException);
                    throw completionException;
                }
            }
        }
    }

    private List<ConsumerRecord<K, V>> processConsumerRecordsBatch(
            List<CompletableFuture<ConsumerRecord<K, V>>> futures) {
        return futures.stream().map(CompletableFuture::join).filter(Objects::nonNull).collect(Collectors.toList());
    }

    private Set<ConsumerRecord<K, V>> getConcurrentSet() {
        return ConcurrentHashMap.newKeySet();
    }

    private CompletableFuture<ConsumerRecord<K, V>> createRecordFuture(
            final ConsumerRecord<K, V> kafkaRecord, final Vector<ConsumerRecord<K, V>> processedRecords,
            final boolean isDeadLetterRecord) {

        ExecutorService recordExecutor = keyPartitionedExecutors.getExecutorForKey(kafkaRecord.key());
        final IConcurrentKafkaConsumer<K, V> recordConsumer = concurrentPartitionConsumerConfig.getRecordConsumer();

        return CompletableFuture.supplyAsync(() -> {

            if (!isDeadLetterRecord && !deadLetterQueue.getOrDefault(kafkaRecord.key(), getConcurrentSet()).isEmpty()) {
                log.warn("[{}] Breaking execution as key {} is in the dead letter queue, saved for later processing.",
                        getPartitionKey(), kafkaRecord.key());
                deadLetterQueue.get(kafkaRecord.key()).add(kafkaRecord);
                return null;
            }

            try {
                ConsumerRecord<K, V> record = recordConsumer.consume(kafkaRecord);
                processedRecords.add(record);

                if (isDeadLetterRecord) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Cleaning in dead letter queue mode {} => {}", getPartitionKey(), kafkaRecord.key(), deadLetterQueue.keySet());
                    }
                    Set<ConsumerRecord<K, V>> queue = deadLetterQueue.get(kafkaRecord.key());
                    if (queue != null) {
                        queue.remove(kafkaRecord);
                        if (queue.isEmpty()) {
                            deadLetterQueue.remove(kafkaRecord.key());
                        }
                    }
                }
                return record;
            } catch (Throwable throwable) {
                deadLetterQueue.computeIfAbsent(kafkaRecord.key(), k -> getConcurrentSet());
                deadLetterQueue.get(kafkaRecord.key()).add(kafkaRecord);
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
