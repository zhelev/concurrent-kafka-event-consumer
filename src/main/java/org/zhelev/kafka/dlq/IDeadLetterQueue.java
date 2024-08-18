package org.zhelev.kafka.dlq;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Collection;
import java.util.Map;

public interface IDeadLetterQueue<K, V> {

    void enqueue(ConsumerRecord<K, V> record);

    void evict(ConsumerRecord<K, V> record);

    void requeue(ConsumerRecord<K, V> record) throws NoSuchDeadLetterException;

    boolean isBlocked(K key);

    boolean block(K key);

    boolean unblock(K key);

    int sizeBlocked();

    void clearBlocks();

    int size();

    Map<K, Integer> summary();

    int queueSize(K key);

    int numberOfQueues();

    void clear();

    boolean contains(K key);

    Iterable<ConsumerRecord<K, V>> queue(K key);

    Iterable<Iterable<ConsumerRecord<K, V>>> queues();

    Collection<K> keys();

    default boolean enqueueIfKeyPresent(ConsumerRecord<K, V> record) {
        if (!contains(record.key())) {
            return false;
        }
        enqueue(record);
        return true;
    }

    default boolean isFull() {
        return false;
    }

    default boolean isQueueFull(K key) {
        return false;
    }

}
