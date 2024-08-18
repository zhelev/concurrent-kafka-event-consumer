package org.zhelev.kafka.dlq;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;

public class InMemoryDeadLetterQueue<K, V> implements IDeadLetterQueue<K, V> {

    private static final Logger log = LoggerFactory.getLogger(InMemoryDeadLetterQueue.class);

    private final Map<K, Deque<ConsumerRecord<K, V>>> deadLetters = new ConcurrentHashMap<>();

    private final Set<K> blockedKeys = ConcurrentHashMap.newKeySet();

    private final int maxQueues;
    private final int maxQueueSize;

    public InMemoryDeadLetterQueue() {
        maxQueues = Integer.MAX_VALUE;
        maxQueueSize = Integer.MAX_VALUE;
    }

    public InMemoryDeadLetterQueue(int maxQueues, int maxQueueSize) {
        this.maxQueues = maxQueues;
        this.maxQueueSize = maxQueueSize;
    }

    @Override
    public void enqueue(ConsumerRecord<K, V> record) {

        if (isQueueFull(record.key())) {
            throw new DeadLetterQueueOverflowException(record.key());
        }

        synchronized (deadLetters) {
            Deque<ConsumerRecord<K, V>> queue = deadLetters.computeIfAbsent(record.key(), id -> new ConcurrentLinkedDeque<>());
            if (!queue.contains(record)) {
                queue.addLast(record);
            } else {
                log.warn("Record is already in the queue {}", record.key());
            }
        }
    }

    @Override
    public void requeue(ConsumerRecord<K, V> record) throws NoSuchDeadLetterException {
        Optional<Map.Entry<K, Deque<ConsumerRecord<K, V>>>> optionalQueue =
                deadLetters.entrySet()
                        .stream()
                        .filter(queue -> queue.getValue().remove(record))
                        .findFirst();

        if (optionalQueue.isPresent()) {
            synchronized (deadLetters) {
                K key = optionalQueue.get().getKey();
                deadLetters.get(key).addFirst(record);
                if (log.isTraceEnabled()) {
                    log.trace("Re-queued record [{}].", key);
                }
            }
        } else {
            throw new NoSuchDeadLetterException(
                    "Cannot requeue [" + record.key() + "] since there is not matching entry in this queue."
            );
        }
    }

    @Override
    public boolean block(K key) {
        return blockedKeys.add(key);
    }

    @Override
    public boolean unblock(K key) {
        return blockedKeys.remove(key);
    }

    @Override
    public int sizeBlocked() {
        return blockedKeys.size();
    }

    @Override
    public void clearBlocks() {
        blockedKeys.clear();
    }

    @Override
    public boolean isBlocked(K key) {
        return blockedKeys.contains(key);
    }

    @Override
    public void evict(ConsumerRecord<K, V> record) {
        Optional<Map.Entry<K, Deque<ConsumerRecord<K, V>>>> optionalQueue =
                deadLetters.entrySet()
                        .stream()
                        .filter(queue -> queue.getValue().remove(record))
                        .findFirst();

        if (optionalQueue.isPresent()) {
            synchronized (deadLetters) {
                K key = optionalQueue.get().getKey();
                if (deadLetters.get(key).isEmpty()) {
                    log.trace("Queue with id [{}] is empty and will be removed.", key);
                    deadLetters.remove(key);
                }
                if (log.isTraceEnabled()) {
                    log.trace("Evicted letter for key [{}].", key);
                }
            }
        } else if (log.isDebugEnabled()) {
            log.debug("Cannot evict record with key [{}] as it could not be found in this queue.", record.key());
        }
    }

    public Map<K, Integer> summary() {
         return deadLetters.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().size()));
    }

    @Override
    public int size() {
        return deadLetters.values()
                .stream()
                .mapToInt(Deque::size)
                .sum();
    }

    @Override
    public int queueSize(K key) {
        return contains(key) ? deadLetters.get(key).size() : 0;
    }

    @Override
    public int numberOfQueues() {
        return deadLetters.size();
    }

    @Override
    public void clear() {
        List<K> queuesToClear = new ArrayList<>(deadLetters.keySet());

        queuesToClear.forEach(key -> {
            deadLetters.get(key).clear();
            deadLetters.remove(key);
        });
    }

    @Override
    public boolean contains(K key) {
        synchronized (deadLetters) {
            return deadLetters.containsKey(key);
        }
    }

    @Override
    public Iterable<ConsumerRecord<K, V>> queue(K key) {
        return contains(key) ? new ArrayList<>(deadLetters.get(key)) : Collections.emptyList();
    }

    @Override
    public Iterable<Iterable<ConsumerRecord<K, V>>> queues() {
        return new ArrayList<>(deadLetters.values());
    }

    @Override
    public Collection<K> keys() {
        return new HashSet<>(deadLetters.keySet());
    }

    @Override
    public boolean enqueueIfKeyPresent(ConsumerRecord<K, V> record) {
        return IDeadLetterQueue.super.enqueueIfKeyPresent(record);
    }

    @Override
    public boolean isFull() {
        return deadLetters.keySet().size() >= maxQueues;
    }

    @Override
    public boolean isQueueFull(K key) {
        return maximumNumberOfQueuesReached(key) || maximumQueueSizeReached(key);
    }

    private boolean maximumNumberOfQueuesReached(K key) {
        return !deadLetters.containsKey(key) && deadLetters.keySet().size() >= maxQueues;
    }

    private boolean maximumQueueSizeReached(K key) {
        return deadLetters.containsKey(key) && deadLetters.get(key).size() >= maxQueueSize;
    }
}
