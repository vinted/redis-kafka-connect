package com.vinted.kafka.connect.redis.feeder;

import com.vinted.kafka.connect.redis.RedisSinkConnectorConfig;
import com.vinted.kafka.connect.redis.converter.KeyConverter;
import com.vinted.kafka.connect.redis.converter.ValueConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import redis.clients.jedis.PipelineBase;
import redis.clients.jedis.Response;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.params.SetParams;

import java.util.Collection;
import java.util.Map;

public class RedisStringFeeder implements IFeeder {
    private final ValueConverter valueConverter;
    private final KeyConverter keyConverter;
    private final UnifiedJedis redis;
    private final RedisSinkConnectorConfig config;

    public RedisStringFeeder(UnifiedJedis redis, RedisSinkConnectorConfig config) {
        this.config = config;
        this.valueConverter = new ValueConverter();
        this.keyConverter = new KeyConverter();
        this.redis = redis;
    }

    @Override
    public void feed(Collection<SinkRecord> collection) {
        try (PipelineBase pipeline = config.isRedisPipelined() ? redis.pipelined() : null) {
            SetParams params = getSetParams();

            collection.forEach(record -> {
                String key = keyConverter.convert(record);
                String value = valueConverter.convert(record);
                set(key, value, params, pipeline);
            });
        }
    }

    private void set(String key, String value, SetParams params, PipelineBase pipeline) {
        if (pipeline != null) {
            pipeline.set(key, value, params);
        } else {
            redis.set(key, value, params);
        }
    }

    private SetParams getSetParams() {
        SetParams params = new SetParams();

        if (config.getRedisKeyTtl() > -1) {
            params.ex(config.getRedisKeyTtl());
        }

        return params;
    }
}
