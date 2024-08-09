package org.zhelev.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

public class ConcurrentKafkaConsumer<K, V> implements AutoCloseable, ConsumerRebalanceListener {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentKafkaConsumer.class);

    private final Properties consumerProperties;
    private final Duration pollDuration;
    private final Map<String, ConcurrentPartitionConsumerConfig<K, V>> concurrentPartitionConsumerConfigs;
    private final Boolean isAutoCommitEnabled;
    private final ExecutorService executorService;
    private final ConcurrentHashMap<String, ConcurrentPartitionConsumer<K, V>> partitionConsumers;
    private final ConcurrentHashMap<String, OffsetAndMetadata> lastCommittedOffsets;

    public ConcurrentKafkaConsumer(final Properties consumerProperties, final Duration pollDuration,
                                   final Map<String, ConcurrentPartitionConsumerConfig<K, V>> concurrentPartitionConsumerConfigs) {
        this.consumerProperties = consumerProperties;
        this.pollDuration = pollDuration;
        this.concurrentPartitionConsumerConfigs = concurrentPartitionConsumerConfigs;

        this.isAutoCommitEnabled = this.consumerProperties.getProperty("enable.auto.commit", "false").equals("true");
        if (isAutoCommitEnabled) {
            log.warn("Using 'enable.auto.commit=true' is not recommended when processing records concurrently.");
        }
        this.partitionConsumers = new ConcurrentHashMap<>();
        this.lastCommittedOffsets = new ConcurrentHashMap<>();
        this.executorService = Executors.newCachedThreadPool();
    }

    public void consume() {
        try (final Consumer<K, V> consumer = new KafkaConsumer<>(consumerProperties)) {

            consumer.subscribe(this.concurrentPartitionConsumerConfigs.keySet(), this);

            long count = 0;
            long firstBatchArrival = 0;

            while (!Thread.currentThread().isInterrupted()) {

                long start = System.nanoTime();

                log.warn("======> {}", partitionConsumers.keySet());

                ConsumerRecords<K, V> records = consumer.poll(pollDuration);
                int recordCount = records.count();
                if (recordCount > 0) {
                    log.info("{} Start processing records {} ...", partitionConsumers.keySet(), recordCount);
                }

                List<CompletableFuture<List<ConsumerRecord<K, V>>>> futures = new ArrayList<>();
                for (TopicPartition partition : records.partitions()) {

                    List<ConsumerRecord<K, V>> partitionRecords = records.records(partition);
                    final ConcurrentPartitionConsumer<K, V> concurrentPartitionConsumer = getConcurrentPartitionConsumer(partition);
                    CompletableFuture<List<ConsumerRecord<K, V>>> future = CompletableFuture.supplyAsync(() -> {
                                final String partitionKey = ConcurrentPartitionConsumer.getPartitionKey(partition);
                                try {
                                    Thread.currentThread().setName("cpc-" + partitionKey);
                                    return concurrentPartitionConsumer.consume(partitionRecords);
                                } catch (CompletionException completionException) {
                                    if (completionException.getCause() instanceof ConcurrentKafkaConsumerException) {
                                        throw (ConcurrentKafkaConsumerException) completionException.getCause();
                                    } else {
                                        try {
                                            partitionConsumers.remove(partitionKey); // next iteration will create a new one
                                            concurrentPartitionConsumer.close();
                                        } catch (Exception e) {
                                            log.warn(e.getMessage(), e);
                                        }
                                    }
                                } catch (ConcurrentPartitionConsumerException cpex) {
                                    log.error(cpex.getMessage(), cpex);
                                    log.error("[{}] Error processing record {}", partitionKey, cpex.getFailedRecord());
                                    log.error("[{}] Successfully processed records in this batch {}", partitionKey, cpex.getProcessedRecords().size());
                                    log.error("[{}] Failed partition records count: {}", partitionKey,
                                            (partitionRecords.size() - cpex.getProcessedRecords().size()));
                                    log.error("[{}] Last successfully commited offset: {}", partitionKey, lastCommittedOffsets.get(partitionKey));
                                    try {
                                        partitionConsumers.remove(partitionKey); // next iteration will create a new one
                                        concurrentPartitionConsumer.close();
                                    } catch (Exception e) {
                                        log.warn(e.getMessage(), e);
                                    }
                                } catch (Throwable ex) {
                                    log.error(ex.getMessage(), ex);
                                    try {
                                        partitionConsumers.remove(partitionKey); // next iteration will create a new one
                                        concurrentPartitionConsumer.close();
                                    } catch (Exception e) {
                                        log.warn(e.getMessage(), e);
                                    }
                                } finally {
                                    Thread.currentThread().setName("cpc-free-" + Thread.currentThread().getId());
                                }
                                log.warn("[{}] Returning an empty list", partitionKey);
                                return new ArrayList<>();
                            }
                            ,
                            executorService
                    );

                    futures.add(future);
                }

                Integer processedKafkaBatchRecords = commitPartitionRecords(consumer, futures);
                count += processedKafkaBatchRecords;
                if (processedKafkaBatchRecords > 0) {
                    if (firstBatchArrival == 0) {
                        firstBatchArrival = start;
                    }
                    long end = System.nanoTime();
                    long duration = end - start;
                    long processingTime = end - firstBatchArrival;
                    double throughput = (double) count * 1_000_000_000 / processingTime;
                    log.info("{} Processing {} records took {} ms", partitionConsumers.keySet(), processedKafkaBatchRecords, TimeUnit.NANOSECONDS.toMillis(duration));
                    log.info("{} Consumed records so far {}, throughput is {} [rec/sec]", partitionConsumers.keySet(), count, String.format("%.2f", throughput));
                }

            }
        }
    }

    private Integer commitPartitionRecords(Consumer<K, V> consumer, List<CompletableFuture<List<ConsumerRecord<K, V>>>> futures) {

        try {
            List<List<ConsumerRecord<K, V>>> records = futures.stream().map(CompletableFuture::join).toList();
            records.forEach(partitionRecords -> {
                commitPartitionOffset(consumer, partitionRecords, isAutoCommitEnabled);
            });
            return records.stream().map(Collection::size).reduce(0, Integer::sum);
        } catch (CompletionException completionException) {
            log.error(completionException.getMessage(), completionException.getCause());
            if (completionException.getCause() instanceof ConcurrentPartitionConsumerException) {
                ConcurrentPartitionConsumerException cpex = (ConcurrentPartitionConsumerException) completionException.getCause();
                String partitionKey = ConcurrentPartitionConsumer.getPartitionKey(cpex.getTopicPartition());
                ConcurrentPartitionConsumer<K,V> concurrentPartitionConsumer = partitionConsumers.remove(partitionKey);
                if(concurrentPartitionConsumer !=null){
                    concurrentPartitionConsumer.close();
                }
            } else {
                throw completionException;
            }
        }catch(ConcurrentPartitionConsumerException cpe){
            try {
                String partitionKey = ConcurrentPartitionConsumer.getPartitionKey(cpe.getTopicPartition());
                ConcurrentPartitionConsumer<K,V> concurrentPartitionConsumer = partitionConsumers.remove(partitionKey);
                if(concurrentPartitionConsumer !=null){
                    concurrentPartitionConsumer.close();
                }
            } catch (Exception e) {
                log.warn(e.getMessage(), e);
            }
        }catch (Exception e){
            log.error(e.getMessage(), e);
        }

        return 0;
    }

    private void commitPartitionOffset(Consumer<K, V> consumer, List<ConsumerRecord<K, V>> partitionRecords, Boolean isAutoCommitEnabled) {

        if (!isAutoCommitEnabled && !partitionRecords.isEmpty()) {
            TopicPartition topicPartition = new TopicPartition(partitionRecords.get(0).topic(), partitionRecords.get(0).partition());
            long lastOffset = partitionRecords.stream().max(Comparator.comparingLong(ConsumerRecord::offset)).get().offset();
            consumer.commitAsync(
                    Collections.singletonMap(topicPartition, new OffsetAndMetadata(lastOffset + 1)),
                    (commit, err) -> {
                        String messsage = "commit " + commit + ", last offset " + lastOffset + ", records count " + partitionRecords.size();
                        if (err == null) {

                            commit.forEach((k, v) -> {
                                String partitionKey = ConcurrentPartitionConsumer.getPartitionKey(k);
                                lastCommittedOffsets.put(partitionKey, v);
                            });

                            log.debug("Successfully commited => {}", messsage);
                        } else {
                            String errMsg = "Error on commit " + messsage;
                            log.error(errMsg);
                            throw new RuntimeException(errMsg, err);
                        }
                    });
        } else {
            log.trace("Messages will be auto commited");
        }
    }

    private ConcurrentPartitionConsumer<K, V> getConcurrentPartitionConsumer(TopicPartition partition) {
        final String partitionKey = ConcurrentPartitionConsumer.getPartitionKey(partition);
        return getConcurrentPartitionConsumer(partition, partitionKey);
    }

    private ConcurrentPartitionConsumer<K, V> getConcurrentPartitionConsumer(TopicPartition partition, String partitionKey) {
        ConcurrentPartitionConsumer<K, V> concurrentPartitionConsumer = partitionConsumers.get(partitionKey);
        if (concurrentPartitionConsumer == null) {
            concurrentPartitionConsumer = new ConcurrentPartitionConsumer<K, V>(partition, this.concurrentPartitionConsumerConfigs.get(partition.topic()));
            partitionConsumers.put(partitionKey, concurrentPartitionConsumer);
        }
        return concurrentPartitionConsumer;
    }

    @Override
    public void close() throws Exception {
        this.partitionConsumers.forEach((k, v) -> {
            log.warn("Closing consumer for partition {}", k);
            try {
                v.close();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        });
        this.executorService.shutdownNow();
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        disconnectPartitionHandlers(collection);
    }

    private void disconnectPartitionHandlers(Collection<TopicPartition> collection) {
        collection.forEach(k -> {
            String partitionKey = ConcurrentPartitionConsumer.getPartitionKey(k);
            ConcurrentPartitionConsumer<K, V> cpc = partitionConsumers.get(partitionKey);
            if (cpc != null) {
                partitionConsumers.remove(partitionKey);
                try {
                    cpc.close();
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }

        });
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        log.info("Got assigned some new partitions: {}", collection);
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        disconnectPartitionHandlers(partitions);
        ConsumerRebalanceListener.super.onPartitionsLost(partitions);
    }
}
