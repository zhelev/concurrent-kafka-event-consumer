package org.zhelev.kafka.dlq;

public class NoSuchDeadLetterException extends RuntimeException {
    public NoSuchDeadLetterException(String message) {
        super(message);
    }
}
