/**
 * A class that manages a set of thread pools, each partitioned by a given key.
 *
 * @author krasimir.zhelev@gmail.com
 */
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

/**
 * A class that manages a set of thread pools, each partitioned by a given key.
 *
 * @author krasimir.zhelev@gmail.com
 */
public class KeyPartitionedExecutors implements AutoCloseable {

    public static final long KEEP_ALIVE_TIME = 0L;

    private final List<ThreadPoolExecutor> executors = new ArrayList<>();

    private final int executorSize;

    private final int threadsPerExecutor;

    private final int executorsQueueSize;

    private final String threadPrefix;

    private final Thread.UncaughtExceptionHandler exceptionHandler;

    /**
     * Creates a new instance of the KeyPartitionedExecutors class with the specified number of executors,
     * threads per executor, queue size, and thread prefix.
     *
     * @param executorCount      The number of executors to create
     * @param threadsPerExecutor The number of threads per executor
     * @param executorQueueSize  The size of the queue for each executor
     * @param threadPrefix       A prefix to use when naming threads
     * @param exceptionHandler   UncaughtExceptionHandler used for all threads
     */
    public KeyPartitionedExecutors(int executorCount, int threadsPerExecutor, int executorQueueSize,
                                   String threadPrefix, Thread.UncaughtExceptionHandler exceptionHandler) {
        this.executorSize = executorCount;
        this.threadsPerExecutor = threadsPerExecutor;
        this.executorsQueueSize = executorQueueSize;
        this.threadPrefix = threadPrefix;
        this.exceptionHandler = exceptionHandler;

        initExecutors();
    }

    /**
     * Initializes the thread pool executors with the specified number of threads and queue sizes.
     * <p>
     * This method creates a new instance of each executor, initializes it with the given parameters,
     * and adds it to the list of executors managed by this class.
     *
     * @see #createThreadPoolExecutor(int)
     */
    private void initExecutors() {
        for (int i = 0; i < executorSize; i++) {
            final ThreadPoolExecutor threadedExecutor = createThreadPoolExecutor(i);
            this.executors.add(threadedExecutor);
        }
    }


    /**
     * Creates and returns a new ThreadFactory instance that names threads with a given prefix and index format.
     *
     * @param prefix           The prefix to use when naming threads
     * @param idxFormat        A format string used to generate thread names, where {0} will be replaced with the thread index
     * @param exceptionHandler An uncaught exception handler to set for each thread created by this factory
     * @return A new ThreadFactory instance
     */
    public static ThreadFactory newThreadFactory(final String prefix, final String idxFormat,
                                                 Thread.UncaughtExceptionHandler exceptionHandler) {

        return new ThreadFactory() {
            private final AtomicLong threadIndex = new AtomicLong(0);

            @Override
            public Thread newThread(Runnable runnable) {
                Thread thread = new Thread(runnable);
                thread.setName(prefix + String.format(idxFormat, threadIndex.getAndIncrement()));
                thread.setUncaughtExceptionHandler(exceptionHandler);
                return thread;
            }
        };
    }

    /**
     * Creates and returns a new ThreadPoolExecutor instance with a given index.
     *
     * @param index The index of the executor to create
     * @return A new ThreadPoolExecutor instance
     */
    private ThreadPoolExecutor createThreadPoolExecutor(int index) {
        final LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(this.executorsQueueSize);

        final String prefix = String.format(threadPrefix + "-p(%03d/%03d)q[%03d])", index, this.executorSize, this.executorsQueueSize);
        final ThreadFactory threadFactory = newThreadFactory(prefix, "-t%01d", this.exceptionHandler);
        return new ThreadPoolExecutor(this.threadsPerExecutor, this.threadsPerExecutor,
                KEEP_ALIVE_TIME, TimeUnit.MILLISECONDS, queue, threadFactory);
    }

    /**
     * Returns the number of executors managed by this class.
     *
     * @return The number of executors
     */
    public int getExecutorSize() {
        return executorSize;
    }

    /**
     * Returns the queue size of the executors.
     *
     * @return The size of the queue for used by all executors
     */
    public int getExecutorsQueueSize() {
        return executorsQueueSize;
    }

    /**
     * Returns the number of threads per executor.
     *
     * @return The number of threads per executor
     */
    public int getThreadsPerExecutor() {
        return threadsPerExecutor;
    }

    /**
     * Returns a list of queue loads, where each load represents the current size of an executor's queue as a fraction of its maximum size.
     *
     * @return A list of float values representing the queue loads
     */
    public List<Float> getQueueLoads() {
        return this.executors.stream().map(executor -> ((float) executor.getQueue().size() / this.executorsQueueSize)).collect(Collectors.toList());
    }

    /**
     * Checks if any of the executors' queues are full.
     *
     * This method checks each executor's queue size and returns true if any of them match the current maximum queue size.
     *
     * @return true if any executor's queue is full, false otherwise
     */
    public Boolean isAnyExecutorQueueFull() {
        return this.executors.stream().anyMatch(executor -> this.executorsQueueSize == executor.getQueue().size());
    }

    /**
     * Returns the maximum queue load among all executors.
     *
     * @return The maximum queue load as a float value
     */
    public Float getMaxQueueLoad() {
        return Collections.max(getQueueLoads());
    }

    /**
     * Returns a list of queue sizes, where each size represents the current size of an executor's queue.
     *
     * @return A list of integer values representing the queue sizes
     */
    public List<Integer> getQueueSizes() {
        return this.executors.stream().map(executor -> executor.getQueue().size()).toList();
    }

    /**
     * Returns the maximum queue size among all executors.
     *
     * @return The maximum queue size as an integer value
     */
    public Integer getMaxQueueSize() {
        return Collections.max(getQueueSizes());
    }

    public ExecutorService getExecutorForKey(Object key) {
        return executors.get(getExecutorIndex(key));
    }

    protected int getExecutorIndex(Object key) {
        int hashCode = key.hashCode();
        int executorIndex = Math.abs(hashCode % this.executorSize);
        if (executorIndex >= this.executorSize) { // should never happen
            executorIndex = 0; // pin to executor 0 (can cause congestion)
        }

        return executorIndex;
    }

    /**
     * Returns an executor service instance associated with a given key.
     * <p>
     * The returned executor service is responsible for managing threads and queues for tasks submitted to it.
     * The choice of which executor service to return depends on the provided key, which is used to determine
     * the index of the desired executor in the list of executors managed by this class.
     *
     * @param key a unique identifier for the desired executor
     * @return an instance of ExecutorService associated with the given key
     */
    public ExecutorService getExecutor(Object key) {
        return executors.get(getExecutorIndex(key));
    }

    /**
     * Shuts down all executors managed by this class.
     *
     * @see ExecutorService#shutdown()
     */
    public void shutDown() {
        this.executors.forEach(executor -> {
            if (!executor.isShutdown()) {
                executor.shutdown();
            }
        });
    }

    /**
     * Closes this KeyPartitionedExecutors instance by shutting down all managed executors.
     *
     * @see AutoCloseable#close()
     */
    @Override
    public void close() {
        shutDown();
    }
}
