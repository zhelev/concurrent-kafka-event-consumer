package org.zhelev.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class ConcurrentKafkaConsumer<K, V> implements AutoCloseable, ConsumerRebalanceListener {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentKafkaConsumer.class);

    private final Properties consumerProperties;
    private final Duration pollDuration;
    private final Map<String, ConcurrentPartitionConsumerConfig> concurrentPartitionConsumerConfigs;
    private final Boolean isAutoCommitEnabled;
    private ExecutorService executorService;
    private ConcurrentHashMap<String, ConcurrentPartitionConsumer> partitionConsumers;

    public ConcurrentKafkaConsumer(Properties consumerProperties, Duration pollDuration,
                                   Map<String, ConcurrentPartitionConsumerConfig> concurrentPartitionConsumerConfigs) {
        this.consumerProperties = consumerProperties;
        this.pollDuration = pollDuration;
        this.concurrentPartitionConsumerConfigs = concurrentPartitionConsumerConfigs;

        this.isAutoCommitEnabled = this.consumerProperties.getProperty("enable.auto.commit", "false").equals("true");
        if (isAutoCommitEnabled) {
            log.warn("Using 'enable.auto.commit=true' is not recommended when processing records concurrently.");
        }
        this.executorService = Executors.newCachedThreadPool();
    }


    public void consume() {
        try (final Consumer<K, V> consumer = new KafkaConsumer<>(consumerProperties)) {

            consumer.subscribe(this.concurrentPartitionConsumerConfigs.keySet(), this);

            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<K, V> records = consumer.poll(pollDuration);

                for (TopicPartition partition : records.partitions()) {

                    List<ConsumerRecord<K, V>> partitionRecords = records.records(partition);

                    ConcurrentPartitionConsumerConfig partitionConsumerConfig = concurrentPartitionConsumerConfigs.get(partition.topic());

                    final String partitionKey = ConcurrentPartitionConsumer.getPartitionKey(partition);
                    final ConcurrentPartitionConsumer concurrentPartitionConsumer = getConcurrentPartitionConsumer(partition, partitionKey, consumer);

                    CompletableFuture.runAsync(() -> {
                                try {
                                    Thread.currentThread().setName("cpc-" + partitionKey);
                                    concurrentPartitionConsumer.consume(partitionRecords, this.isAutoCommitEnabled);
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
                            }
                            ,
                            executorService
                    );

                }
            }
        }
    }

    private ConcurrentPartitionConsumer getConcurrentPartitionConsumer(TopicPartition partition, String partitionKey, Consumer<K, V> consumer) {
        ConcurrentPartitionConsumer concurrentPartitionConsumer = partitionConsumers.get(partitionKey);
        if (concurrentPartitionConsumer == null) {
            concurrentPartitionConsumer = new ConcurrentPartitionConsumer<K, V>(consumer, partition, this.concurrentPartitionConsumerConfigs.get(partition.topic()));
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
