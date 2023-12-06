package com.vinted.kafka.connect.redis;

import org.apache.kafka.connect.connector.Task;

import java.util.Map;

public class RedisSinkTask implements Task {
    @Override
    public String version() {
        return RedisSinkConnectorVersion.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {

    }

    @Override
    public void stop() {

    }
}
