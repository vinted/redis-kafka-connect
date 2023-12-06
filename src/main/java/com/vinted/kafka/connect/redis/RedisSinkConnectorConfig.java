package com.vinted.kafka.connect.redis;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class RedisSinkConnectorConfig extends AbstractConfig {

    public static final String REDIS_URI = "redis.uri";
    public static final String REDIS_CLUSTER = "redis.cluster";
    public static final String REDIS_TYPE = "redis.type";
    public static final String REDIS_KEY = "redis.key";
    public static final String REDIS_CONNECT_TIMEOUT = "redis.socket.connect.timeout.ms";

    public RedisSinkConnectorConfig(Map<?, ?> originals) {
        super(configDef(), originals);
    }

    protected static ConfigDef configDef() {
        return new ConfigDef()
                .define(REDIS_URI,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        "")
                .define(REDIS_CLUSTER,
                        ConfigDef.Type.BOOLEAN,
                        ConfigDef.Importance.MEDIUM,
                        "Whitelist of object key prefixes")
                .define(REDIS_TYPE,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        "Whitelist of object key prefixes")
                .define(REDIS_KEY,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.LOW,
                        "Name of Kafka topic to produce to")
                .define(REDIS_CONNECT_TIMEOUT,
                        ConfigDef.Type.INT,
                        ConfigDef.Importance.LOW,
                        "Name of Kafka topic to produce to");
    }
}
