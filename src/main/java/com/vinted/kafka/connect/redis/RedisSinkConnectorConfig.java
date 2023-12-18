package com.vinted.kafka.connect.redis;

import com.google.common.base.Strings;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class RedisSinkConnectorConfig extends AbstractConfig {
    public static final String TOKEN_TOPIC = "${topic}";
    public static final String REDIS_URI = "redis.uri";
    public static final String REDIS_CLUSTER = "redis.cluster";
    public static final String REDIS_PIPELINED = "redis.pipelined";
    public static final String REDIS_TYPE = "redis.type";
    public static final String REDIS_KEY = "redis.key";
    public static final String REDIS_KEY_DOC = "A format string for destination key space, which may contain '" + TOKEN_TOPIC
            + "' as a placeholder for the originating topic name.\nFor example, ``kafka_" + TOKEN_TOPIC
            + "`` for the topic 'orders' will map to the Redis key space "
            + "'kafka_orders'.\nLeave empty for passthrough (only applicable to non-collection data structures).";
    public static final String REDIS_KEY_SEPARATOR = "redis.key.separator";
    public static final String REDIS_KEY_SEPARATOR_DEFAULT = ":";
    public static final String REDIS_KEY_TTL = "redis.key.ttl";
    public static final String REDIS_CONNECT_TIMEOUT = "redis.socket.connect.timeout.ms";
    private final String redisUri;
    private final boolean isRedisCluster;
    private final boolean isRedisPipelined;
    private final String redisType;
    private final String redisKeyspace;
    private final String redisKeySeparator;
    private final Integer redisTimeout;
    private final Integer redisKeyTtl;

    public RedisSinkConnectorConfig(Map<?, ?> originals) {
        super(configDef(), originals);
        this.redisUri = getString(RedisSinkConnectorConfig.REDIS_URI);
        this.isRedisCluster = Boolean.TRUE.equals(getBoolean(RedisSinkConnectorConfig.REDIS_CLUSTER));
        this.isRedisPipelined = Boolean.TRUE.equals(getBoolean(RedisSinkConnectorConfig.REDIS_PIPELINED));
        this.redisType = getString(RedisSinkConnectorConfig.REDIS_TYPE);
        this.redisKeyspace = getString(RedisSinkConnectorConfig.REDIS_KEY);
        this.redisKeySeparator = getString(RedisSinkConnectorConfig.REDIS_KEY_SEPARATOR);
        this.redisKeyTtl = getInt(RedisSinkConnectorConfig.REDIS_KEY_TTL);
        this.redisTimeout = getInt(RedisSinkConnectorConfig.REDIS_CONNECT_TIMEOUT);
    }

    protected static ConfigDef configDef() {
        return new ConfigDef()
                .define(REDIS_URI,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        "Redis URI or Redis cluster node URIs, separated by semicolon (';')")
                .define(REDIS_CLUSTER,
                        ConfigDef.Type.BOOLEAN,
                        false,
                        ConfigDef.Importance.MEDIUM,
                        "Whitelist of object key prefixes")
                .define(REDIS_PIPELINED,
                        ConfigDef.Type.BOOLEAN,
                        false,
                        ConfigDef.Importance.MEDIUM,
                        "Pipelined redis calls")
                .define(REDIS_TYPE,
                        ConfigDef.Type.STRING,
                        "string",
                        ConfigDef.Importance.HIGH,
                        "Whitelist of object key prefixes")
                .define(REDIS_KEY,
                        ConfigDef.Type.STRING,
                        "",
                        ConfigDef.Importance.LOW,
                        REDIS_KEY_DOC)
                .define(REDIS_KEY_SEPARATOR,
                        ConfigDef.Type.STRING,
                        REDIS_KEY_SEPARATOR_DEFAULT,
                        ConfigDef.Importance.LOW,
                        "Separator keyspace and key")
                .define(REDIS_KEY_TTL,
                        ConfigDef.Type.INT,
                        -1,
                        ConfigDef.Importance.LOW,
                        "Redis key TTL in seconds")
                .define(REDIS_CONNECT_TIMEOUT,
                        ConfigDef.Type.INT,
                        10000,
                        ConfigDef.Importance.LOW,
                        "Redis connect timeout");
    }

    public String getRedisKeyPrefix() {
        if (Strings.isNullOrEmpty(redisKeyspace)) {
            return "";
        } else {
            return redisKeyspace + redisKeySeparator;
        }
    }
    public String getRedisUri() {
        return redisUri;
    }

    public boolean isRedisCluster() {
        return isRedisCluster;
    }

    public String getRedisType() {
        return redisType;
    }

    public Integer getRedisTimeout() {
        return redisTimeout;
    }

    public boolean isRedisPipelined() {
        return isRedisPipelined;
    }

    public Integer getRedisKeyTtl() {
        return redisKeyTtl;
    }
}
