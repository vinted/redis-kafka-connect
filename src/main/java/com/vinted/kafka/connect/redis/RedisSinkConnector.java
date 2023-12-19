package com.vinted.kafka.connect.redis;


import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.*;

public class RedisSinkConnector extends SinkConnector {
    private Map<String, String> props;

    @Override
    public void start(Map<String, String> props) {
        this.props = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return RedisSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return Collections.nCopies(maxTasks, props);
    }

    @Override
    public void stop() {
        // Do nothing.
    }

    @Override
    public ConfigDef config() {
        return RedisSinkConnectorConfig.configDef();
    }

    @Override
    public String version() {
        return RedisSinkConnectorVersion.getVersion();
    }
}
