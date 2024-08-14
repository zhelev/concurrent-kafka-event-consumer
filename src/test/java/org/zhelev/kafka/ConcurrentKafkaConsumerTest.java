package org.zhelev.kafka;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zhelev.kafka.partition.ConcurrentPartitionConsumerConfig;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;

public class ConcurrentKafkaConsumerTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentKafkaConsumer.class);

    // Consumer config
    private static final String BOOTSTRAP_SERVERS = "192.168.1.170:29092,192.168.1.170:39092,192.168.1.170:49092";
    private static final String CONSUMER_GROUP_ID = "kafka-concurrent-consumer";
    private static final Properties CONSUMER_PROPS = new Properties() {{
        put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        put(GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        put(ENABLE_AUTO_COMMIT_CONFIG, "false"); // false is recommended
    }};

    // ConcurrentKafkaConsumer class config
    private static final String TOPIC = "idmusers";
    private static final List<String> TOPICS = List.of(TOPIC);
    private static final Duration POLL_DURATION = Duration.ofMillis(100);
    private static final int EXECUTOR_SIZE = 20; // Runtime.getRuntime().availableProcessors();
    private static final int QUEUES_PER_EXECUTOR = 40;
    private static final int MAX_BATCH_SIZE = EXECUTOR_SIZE * QUEUES_PER_EXECUTOR;


    private static final Integer MAX_SLEEP_TIME = 500;
    private static final Integer MIN_SLEEP_TIME = 100;
    private static final Integer DEF_SLEEP_TIME = 300;

    @Test
    void sleepyTest() {

        ConcurrentHashMap<String, String> registry = new ConcurrentHashMap<>();

        // Sleeping record processor
        IConcurrentKafkaConsumer<String, String> recordConsumer = record -> {
            final String currentThreadName = Thread.currentThread().getName();
            // log.info("Processing record with key {} partition {} on thread {}", record.key(), record.partition(), currentThreadName);
            String threadName = registry.get(record.key());
            if (threadName == null) {
                registry.put(record.key(), currentThreadName);
            } else {
                Assertions.assertEquals(threadName, currentThreadName);
            }

            // this event takes log to process
            try {
                long sleepFor = new Random().nextInt(MIN_SLEEP_TIME, MAX_SLEEP_TIME); // DEF_SLEEP_TIME
                Thread.sleep(sleepFor);
            } catch (InterruptedException e) {
                throw new ConcurrentKafkaConsumerException(e, record);
            }
            return record;
        };

        ConcurrentPartitionConsumerConfig<String, String> concurrentPartitionConsumerConfig = new ConcurrentPartitionConsumerConfig(recordConsumer);
        concurrentPartitionConsumerConfig.setExecutorSize(EXECUTOR_SIZE);
        concurrentPartitionConsumerConfig.setExecutorQueueSize(QUEUES_PER_EXECUTOR);
        concurrentPartitionConsumerConfig.setMaxBatchSize(MAX_BATCH_SIZE);

        Map<String, ConcurrentPartitionConsumerConfig<String, String>> concurrentPartitionConsumerConfigs = new HashMap<>() {{
            put(TOPIC, concurrentPartitionConsumerConfig);
        }};


        try (ConcurrentKafkaConsumer<String, String> kafkaConcurrentConsumer = new ConcurrentKafkaConsumer<>(
                CONSUMER_PROPS, POLL_DURATION, concurrentPartitionConsumerConfigs)) {
            kafkaConcurrentConsumer.consume();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

    }

    @Test
    void basicTest() {

        ConcurrentHashMap<String, String> registry = new ConcurrentHashMap<>();

        // noop record processor
        IConcurrentKafkaConsumer<String, String> recordConsumer = record -> {
            final String currentThreadName = Thread.currentThread().getName();
            // log.info("Processing record with key {} partition {} on thread {}", record.key(), record.partition(), currentThreadName);
            String threadName = registry.get(record.key());
            if (threadName == null) {
                registry.put(record.key(), currentThreadName);
            } else {
                Assertions.assertEquals(threadName, currentThreadName);
            }

            // event processing happens here
            return record;
        };

        ConcurrentPartitionConsumerConfig<String, String> concurrentPartitionConsumerConfig = new ConcurrentPartitionConsumerConfig(recordConsumer);
        concurrentPartitionConsumerConfig.setExecutorSize(EXECUTOR_SIZE);
        concurrentPartitionConsumerConfig.setExecutorQueueSize(QUEUES_PER_EXECUTOR);
        concurrentPartitionConsumerConfig.setMaxBatchSize(MAX_BATCH_SIZE);

        Map<String, ConcurrentPartitionConsumerConfig<String, String>> concurrentPartitionConsumerConfigs = new HashMap<>() {{
            put(TOPIC, concurrentPartitionConsumerConfig);
        }};


        try (ConcurrentKafkaConsumer<String, String> kafkaConcurrentConsumer = new ConcurrentKafkaConsumer<>(
                CONSUMER_PROPS, POLL_DURATION, concurrentPartitionConsumerConfigs)) {
            kafkaConcurrentConsumer.consume();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    @Test
    void errorTest() {

        ConcurrentHashMap<String, String> registry = new ConcurrentHashMap<>();

        final Random rand = new Random();

        // noop record processor
        IConcurrentKafkaConsumer<String, String> recordConsumer = record -> {
            final String currentThreadName = Thread.currentThread().getName();
            // log.info("Processing record with key {} partition {} on thread {}", record.key(), record.partition(), currentThreadName);
            String threadName = registry.get(record.key());
            if (threadName == null) {
                registry.put(record.key(), currentThreadName);
            } else {
                Assertions.assertEquals(threadName, currentThreadName);
            }

            int random = rand.nextInt(0, 100);
            if (random > 120) {
                throw new RuntimeException("I was unlucky!");
                // throw new ConcurrentKafkaConsumerException("I lost database connection", record);
            }


            // event processing happens here
            return record;
        };

        ConcurrentPartitionConsumerConfig<String, String> concurrentPartitionConsumerConfig = new ConcurrentPartitionConsumerConfig(recordConsumer);
        concurrentPartitionConsumerConfig.setExecutorSize(EXECUTOR_SIZE);
        concurrentPartitionConsumerConfig.setExecutorQueueSize(QUEUES_PER_EXECUTOR);
        concurrentPartitionConsumerConfig.setMaxBatchSize(MAX_BATCH_SIZE);

        Map<String, ConcurrentPartitionConsumerConfig<String, String>> concurrentPartitionConsumerConfigs = new HashMap<>() {{
            put(TOPIC, concurrentPartitionConsumerConfig);
        }};


        try (ConcurrentKafkaConsumer<String, String> kafkaConcurrentConsumer = new ConcurrentKafkaConsumer<>(
                CONSUMER_PROPS, POLL_DURATION, concurrentPartitionConsumerConfigs)) {
            kafkaConcurrentConsumer.consume();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

}

