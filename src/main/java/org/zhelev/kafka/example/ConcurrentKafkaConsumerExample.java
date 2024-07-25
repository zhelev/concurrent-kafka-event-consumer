package org.zhelev.kafka.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zhelev.kafka.ConcurrentKafkaConsumer;
import org.zhelev.kafka.ConcurrentKafkaConsumerException;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;

public class ConcurrentKafkaConsumerExample {

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
    private static final Integer EXECUTOR_SIZE = 20; // Runtime.getRuntime().availableProcessors();
    private static final Integer QUEUES_PER_EXECUTOR = 40;

    public static void main(String[] args) {

        // Sleeping record processor
        ConcurrentKafkaConsumer.IConcurrentKafkaConsumer<String, String> recordConsumer = new ConcurrentKafkaConsumer.IConcurrentKafkaConsumer() {

            private ConsumerRecord<String, String> consumerRecord;
            private Random random = new Random();

            // record processing happens here
            @Override
            public ConsumerRecord<String, String> consume(ConsumerRecord record) throws ConcurrentKafkaConsumerException {
                this.consumerRecord = record;
                log.info("Processing record with key {} partition {} on thread {}", record.key(), record.partition(), Thread.currentThread().getName());
                try {
                    int sleep = random.nextInt(100, 2000);
                    Thread.sleep(sleep);
                } catch (InterruptedException e) {
                    throw new ConcurrentKafkaConsumerException(e, record);
                }

                return record;
            }

            @Override
            public Thread.UncaughtExceptionHandler exceptionHandler(Thread t, Throwable e) {
                return (throwable, exception) -> {
                    throw new ConcurrentKafkaConsumerException(exception.getMessage(), this.consumerRecord);
                };
            }
        };

        ConcurrentKafkaConsumer<String, String> kafkaConcurrentConsumer = new ConcurrentKafkaConsumer<>(
                CONSUMER_PROPS, TOPICS, POLL_DURATION, EXECUTOR_SIZE, QUEUES_PER_EXECUTOR, recordConsumer);

        kafkaConcurrentConsumer.consume();
    }

}

