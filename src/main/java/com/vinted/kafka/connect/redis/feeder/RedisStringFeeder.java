package com.vinted.kafka.connect.redis.feeder;

import com.vinted.kafka.connect.redis.RedisSinkConnectorConfig;
import com.vinted.kafka.connect.redis.RedisSinkTask;
import com.vinted.kafka.connect.redis.converter.KeyConverter;
import com.vinted.kafka.connect.redis.converter.ValueConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.PipelineBase;
import redis.clients.jedis.Response;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.params.SetParams;

import java.util.Collection;
import java.util.Map;

public class RedisStringFeeder implements IFeeder {
    private static final Logger log = LoggerFactory.getLogger(RedisStringFeeder.class);
    private final ValueConverter valueConverter;
    private final KeyConverter keyConverter;
    private final UnifiedJedis redis;
    private final RedisSinkConnectorConfig config;

    public RedisStringFeeder(
            UnifiedJedis redis,
            RedisSinkConnectorConfig config,
            KeyConverter keyConverter,
            ValueConverter valueConverter
    ) {
        this.config = config;
        this.valueConverter = valueConverter;
        this.keyConverter = keyConverter;
        this.redis = redis;
    }

    public RedisStringFeeder(UnifiedJedis redis, RedisSinkConnectorConfig config) {
        this(redis, config, new KeyConverter(config), new ValueConverter());
    }

    @Override
    public void feed(Collection<SinkRecord> collection) {
        try (PipelineBase pipeline = config.isRedisPipelined() ? redis.pipelined() : null) {
            SetParams params = getSetParams();

            collection.forEach(record -> {
                byte[] key = keyConverter.convert(record);
                byte[] value = valueConverter.convert(record);

                if (value == null) {
                    delete(key, pipeline);
                } else {
                    set(key, value, params, pipeline);
                }
            });
        }
    }

    private void set(byte[] key, byte[] value, SetParams params, PipelineBase pipeline) {
        if (pipeline != null) {
            pipeline.set(key, value, params);
        } else {
            redis.set(key, value, params);
        }
    }

    private void delete(byte[] key, PipelineBase pipeline) {
        if (pipeline != null) {
            pipeline.expire(key, 1);
        } else {
            redis.expire(key, 1);
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
