package org.zhelev.kafka;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class KeyPartitionedExecutors {

    private final List<ThreadPoolExecutor> executors = new ArrayList<>();

    private final int executorSize;

    private final int queueSize;

    private final String threadPrefix;
    private final Thread.UncaughtExceptionHandler exceptionHandler;

    public KeyPartitionedExecutors(int executorSize, int queueSize, String threadPrefix, Thread.UncaughtExceptionHandler exceptionHandler) {
        this.executorSize = executorSize;
        this.queueSize = queueSize;
        this.threadPrefix = threadPrefix;
        this.exceptionHandler = exceptionHandler;
        initExecutors();
    }

    public static ThreadFactory newThreadFactory(final String prefix, final String idxFormat,
                                                 Thread.UncaughtExceptionHandler exceptionHandler) {
        ThreadFactory threadFactory = new ThreadFactory() {
            private final AtomicLong threadIndex = new AtomicLong(0);

            @Override
            public Thread newThread(Runnable runnable) {
                Thread thread = new Thread(runnable);
                thread.setName(prefix + String.format(idxFormat, threadIndex.getAndIncrement()));
                thread.setUncaughtExceptionHandler(exceptionHandler);
                return thread;
            }
        };

        return threadFactory;
    }


    public int getExecutorSize() {
        return executorSize;
    }

    public int getQueueSize() {
        return queueSize;
    }

    private ThreadPoolExecutor createSingleThreadPoolExecutor(int index) {
        final LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(this.queueSize);

        final String prefix = String.format(threadPrefix + "-part(%03d/%03d)[%03d]-", index, this.executorSize, this.queueSize);
        final ThreadFactory threadFactory = newThreadFactory(prefix, "%01d", this.exceptionHandler);
        final ThreadPoolExecutor threadedExecutor =
                new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, queue, threadFactory);
        return threadedExecutor;
    }

    public List<Float> getQueueLoads() {
        return this.executors.stream().map(executor -> ((float)executor.getQueue().size()/this.queueSize)).collect(Collectors.toList());
    }

    public Float getMaxQueueLoad() {
        return Collections.max(getQueueLoads());
    }

    public List<Integer> getQueueSizes() {
        return this.executors.stream().map(executor -> executor.getQueue().size()).collect(Collectors.toUnmodifiableList());
    }

    public Integer getMaxQueueSize() {
        return Collections.max(getQueueSizes());
    }

    private void initExecutors() {
        for (int i = 0; i < executorSize; i++) {
            final ThreadPoolExecutor threadedExecutor = createSingleThreadPoolExecutor(i);
            this.executors.add(threadedExecutor);
        }
    }

    public ExecutorService getExecutorForKey(Object key) {
        return executors.get(getExecutorIndex(key));
    }

    int getExecutorIndex(Object key) {
        int hashCode = key.hashCode();
        int executorIndex = Math.abs(hashCode % this.executorSize);
        if (executorIndex < 0 || executorIndex >= this.executorSize) {
            executorIndex = 0; // pin to executor 0 (can cause congestion)
        }

        return executorIndex;
    }

    public ExecutorService getExecutor(Object key) {
        return executors.get(getExecutorIndex(key));
    }

    public void shutDown() {
        executors.forEach(executor -> {
            if (executor.isShutdown()) {
                executor.shutdown();
            }
        });
    }

}
