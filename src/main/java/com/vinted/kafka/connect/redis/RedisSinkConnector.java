package com.vinted.kafka.connect.redis;


import com.google.common.collect.ImmutableList;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedisSinkConnector extends SourceConnector {

    private RedisSinkConnectorConfig connectorConfig;

    @Override
    public void start(Map<String, String> props) {
        this.connectorConfig = new RedisSinkConnectorConfig(props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return RedisSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        final List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);

        final Map<String, String> configs = this.connectorConfig.originalsStrings();

        for (int i = 0; i < maxTasks; i++) {
            taskConfigs.add(new HashMap<>(configs));
        }

        return ImmutableList.copyOf(taskConfigs);
    }

    @Override
    public void stop() {
        // Do nothing.
    }

    @Override
    public ConfigDef config() {
        return null;
    }

    @Override
    public String version() {
        return RedisSinkConnectorVersion.getVersion();
    }
}
