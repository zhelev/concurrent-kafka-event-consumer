package org.zhelev.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class ConcurrentKafkaConsumer<K, V> implements AutoCloseable, ConsumerRebalanceListener {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentKafkaConsumer.class);

    private final Properties consumerProperties;
    private final Duration pollDuration;
    private final Map<String, ConcurrentPartitionConsumerConfig> concurrentPartitionConsumerConfigs;
    private final Boolean isAutoCommitEnabled;
    private final ExecutorService executorService;
    private final ConcurrentHashMap<String, ConcurrentPartitionConsumer> partitionConsumers;

    public ConcurrentKafkaConsumer(Properties consumerProperties, Duration pollDuration,
                                   Map<String, ConcurrentPartitionConsumerConfig> concurrentPartitionConsumerConfigs) {
        this.consumerProperties = consumerProperties;
        this.pollDuration = pollDuration;
        this.concurrentPartitionConsumerConfigs = concurrentPartitionConsumerConfigs;

        this.isAutoCommitEnabled = this.consumerProperties.getProperty("enable.auto.commit", "false").equals("true");
        if (isAutoCommitEnabled) {
            log.warn("Using 'enable.auto.commit=true' is not recommended when processing records concurrently.");
        }
        this.partitionConsumers = new ConcurrentHashMap<>();
        this.executorService = Executors.newCachedThreadPool();
    }

    public void consume() {
        try (final Consumer<K, V> consumer = new KafkaConsumer<>(consumerProperties)) {

            consumer.subscribe(this.concurrentPartitionConsumerConfigs.keySet(), this);

            while (!Thread.currentThread().isInterrupted()) {

                long start = System.nanoTime();

                ConsumerRecords<K, V> records = consumer.poll(pollDuration);

                Vector<CompletableFuture<List<ConsumerRecord<K, V>>>> futures = new Vector<>();
                for (TopicPartition partition : records.partitions()) {

                    List<ConsumerRecord<K, V>> partitionRecords = records.records(partition);

                    final String partitionKey = ConcurrentPartitionConsumer.getPartitionKey(partition);
                    final ConcurrentPartitionConsumer concurrentPartitionConsumer = getConcurrentPartitionConsumer(partition, partitionKey, consumer);

                    CompletableFuture<List<ConsumerRecord<K, V>>> future = CompletableFuture.supplyAsync(() -> {
                                try {
                                    Thread.currentThread().setName("cpc-" + partitionKey);
                                    return concurrentPartitionConsumer.consume(partitionRecords);
                                } catch (Exception ex) {
                                    log.error(ex.getMessage(), ex);
                                    try {
                                        partitionConsumers.remove(partitionKey);
                                        concurrentPartitionConsumer.close();
                                    } catch (Exception e) {
                                        log.warn(ex.getMessage(), e);
                                    }
                                } finally {
                                    Thread.currentThread().setName("cpc-free-thread-" + Thread.currentThread().getId());
                                }
                                return new ArrayList<ConsumerRecord<K, V>>();
                            }
                            ,
                            executorService
                    );

                    futures.add(future);
                }
                Integer totalRecords = commitPartitionRecords(consumer, futures);

                if (totalRecords > 0) {
                    long duration = System.nanoTime() - start;
                    log.debug("Processing {} records took {} ms", totalRecords, TimeUnit.NANOSECONDS.toMillis(duration));
                }
            }
        }
    }

    private Integer commitPartitionRecords(Consumer consumer, Vector<CompletableFuture<List<ConsumerRecord<K, V>>>> futures) {

        List<List<ConsumerRecord<K, V>>> records = futures.stream().map(CompletableFuture::join)
                .collect(Collectors.toList());
        records.forEach(partitionRecords -> {
            commitPartitionOffset(consumer, partitionRecords, isAutoCommitEnabled);
        });
        return records.stream().map(Collection::size).reduce(0, Integer::sum);
    }

    private void commitPartitionOffset(Consumer consumer, List<ConsumerRecord<K, V>> partitionRecords, Boolean isAutoCommitEnabled) {

        if (!isAutoCommitEnabled && partitionRecords.size() > 0) {
            TopicPartition topicPartition = new TopicPartition(partitionRecords.get(0).topic(), partitionRecords.get(0).partition());
            long lastOffset = partitionRecords.stream().max(Comparator.comparingLong(ConsumerRecord::offset)).get().offset();
            consumer.commitAsync(
                    Collections.singletonMap(topicPartition, new OffsetAndMetadata(lastOffset + 1)),
                    (commit, err) -> {
                        String messsage = "commit " + commit + ", last offset " + lastOffset + ", records count "+partitionRecords.size();
                        if (err == null) {
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


    private ConcurrentPartitionConsumer getConcurrentPartitionConsumer(TopicPartition partition, String partitionKey, Consumer<K, V> consumer) {
        ConcurrentPartitionConsumer concurrentPartitionConsumer = partitionConsumers.get(partitionKey);
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
            ConcurrentPartitionConsumer cpc = partitionConsumers.get(partitionKey);
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
        log.warn("Got assigned some new partitions: {}", collection);
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        disconnectPartitionHandlers(partitions);
        ConsumerRebalanceListener.super.onPartitionsLost(partitions);
    }
}
