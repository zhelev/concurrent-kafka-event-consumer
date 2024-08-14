package org.zhelev.kafka;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zhelev.kafka.utils.KeyPartitionedExecutors;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

class KeyPartitionedExecutorsTest {

    private static final Logger log = LoggerFactory.getLogger(KeyPartitionedExecutors.class);
    public static final int THREADS_PER_EXECUTOR = 1;

    private KeyPartitionedExecutors keyPartitionedExecutors;
    private int executorSize = 4;
    private int queueSize = 10;
    private String threadPrefix = "my-exec";

    private Thread.UncaughtExceptionHandler exceptionHandler = (t, e) -> {
        log.error("{} => {}", t, e);
    };

    List<String> testKeys = List.of("key1", "key2", "dummy", "key3", "key4", "key5", "krassi", "key1", "key2", "student", "key3", "key4", "key5");

    @BeforeEach
    void setUp() {
        keyPartitionedExecutors = new KeyPartitionedExecutors(executorSize, THREADS_PER_EXECUTOR, queueSize, threadPrefix, exceptionHandler);
    }

    @AfterEach
    void tearDown() {
        keyPartitionedExecutors.shutDown();
    }

    @Test
    void getExecutorForKey() {
        Map<String, String> reg = new ConcurrentHashMap<>();

        testKeys.forEach(key -> {
            ExecutorService recordExecutor = keyPartitionedExecutors.getExecutorForKey(key.toString());
            CompletableFuture.runAsync(() -> {
                String threadName = Thread.currentThread().getName();
                log.info("Key {} => {}", key, threadName);
                if (reg.containsKey(key)) {
                    Assertions.assertEquals(reg.get(key), threadName);
                }else {
                    reg.put(key, threadName);
                }
            }, recordExecutor).join();
        });
    }

    @Test
    void getIndex() {
        Map<String, Integer> reg = new HashMap<>();
        testKeys.forEach(key -> {
                    int execIndex = keyPartitionedExecutors.getExecutorIndex(key);
                    if (reg.containsKey(key)) {
                        Assertions.assertEquals(reg.get(key), execIndex);
                    } else {
                        reg.put(key, execIndex);
                    }
                    log.info("Key {} => {}", key, execIndex);
                    Assertions.assertTrue(executorSize > execIndex);
                }
        );
    }
}
