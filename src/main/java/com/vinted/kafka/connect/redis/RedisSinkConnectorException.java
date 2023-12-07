package com.vinted.kafka.connect.redis;

public class RedisSinkConnectorException extends RuntimeException {
    public RedisSinkConnectorException(String message) {
        super(message);
    }

    public RedisSinkConnectorException(String message, Throwable cause) {
        super(message, cause);
    }
}
