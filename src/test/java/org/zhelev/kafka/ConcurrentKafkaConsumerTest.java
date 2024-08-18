package org.zhelev.kafka;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zhelev.kafka.dlq.InMemoryDeadLetterQueue;
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
    private static final String TOPIC = "test";
    private static final Duration POLL_DURATION = Duration.ofMillis(100);
    private static final int EXECUTOR_SIZE = 20; // Runtime.getRuntime().availableProcessors(); // IO vs CPU blocking
    private static final int QUEUES_PER_EXECUTOR = 40;
    private static final int MAX_RETRY_COUNT = 10;
    private static final int MAX_BATCH_SIZE = EXECUTOR_SIZE * QUEUES_PER_EXECUTOR;


    private static final Integer MAX_SLEEP_TIME = 300;
    private static final Integer MIN_SLEEP_TIME = 50;
    private static final Integer DEF_SLEEP_TIME = 300;
    public static final int SUCCESS_PERCENTAGE_RATE = 95;
    private static final Integer MAX_BLOCKED = 300;

    private static Map<String, ConcurrentPartitionConsumerConfig<String, String>> getDefaultConcurrentPartitionConsumerConfig(IConcurrentKafkaConsumer<String, String> recordConsumer) {
        ConcurrentPartitionConsumerConfig<String, String> concurrentPartitionConsumerConfig = new ConcurrentPartitionConsumerConfig<>(recordConsumer);
        concurrentPartitionConsumerConfig.setExecutorSize(EXECUTOR_SIZE);
        concurrentPartitionConsumerConfig.setExecutorQueueSize(QUEUES_PER_EXECUTOR);
        concurrentPartitionConsumerConfig.setMaxBatchSize(MAX_BATCH_SIZE);
        concurrentPartitionConsumerConfig.setMaxRetryCount(MAX_RETRY_COUNT);
        concurrentPartitionConsumerConfig.setMaxBlocked(MAX_BLOCKED);
        concurrentPartitionConsumerConfig.setDeadLetterQueue(new InMemoryDeadLetterQueue<>());

        return new HashMap<>() {{
            put(TOPIC, concurrentPartitionConsumerConfig);
        }};
    }

    private static void doDefaultConsume(IConcurrentKafkaConsumer<String, String> recordConsumer) {
        Map<String, ConcurrentPartitionConsumerConfig<String, String>> concurrentPartitionConsumerConfigs = getDefaultConcurrentPartitionConsumerConfig(recordConsumer);

        try (ConcurrentKafkaConsumer<String, String> kafkaConcurrentConsumer = new ConcurrentKafkaConsumer<>(
                CONSUMER_PROPS, POLL_DURATION, concurrentPartitionConsumerConfigs)) {
            kafkaConcurrentConsumer.consume();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    /**
     * Normal processing
     * */
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

        doDefaultConsume(recordConsumer);
        registry.clear();
    }


    /**
     * Processing takes
     * */
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

        doDefaultConsume(recordConsumer);
        registry.clear();
    }

    /**
     * Processing takes time and sometimes fails
     */
    @Test
    void errorTest() {

        ConcurrentHashMap<String, String> registry = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, String> failedRecordRegistry = new ConcurrentHashMap<>();
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

            if (Thread.currentThread().isInterrupted()) {
                throw new ConcurrentKafkaConsumerException("Thread interrupted", record);
            }

            // this event takes log to process
            try {
                long sleepFor = new Random().nextInt(MIN_SLEEP_TIME, MAX_SLEEP_TIME); // DEF_SLEEP_TIME
                Thread.sleep(sleepFor);
            } catch (InterruptedException e) {
                throw new ConcurrentKafkaConsumerException(e, record);
            }

            // this event can fail
            int random = rand.nextInt(1, 101);
            if (random > SUCCESS_PERCENTAGE_RATE) {
                failedRecordRegistry.put(record.key(), record.value());
                // throw new RuntimeException("I was unlucky, random is: " + random);
                log.warn("["+record.key()+"] I lost database connection. Fail rate set to " + (100 - SUCCESS_PERCENTAGE_RATE) + "%! I got: "+random);
                throw new ConcurrentKafkaConsumerException("I lost database connection. Fail rate set to " + (100 - SUCCESS_PERCENTAGE_RATE) + "%!", record);
            }

            // event processing happens here
            return record;
        };

        doDefaultConsume(recordConsumer);

        log.warn("Failed records: {}", failedRecordRegistry);
    }

}

