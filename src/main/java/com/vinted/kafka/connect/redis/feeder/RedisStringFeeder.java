package com.vinted.kafka.connect.redis.feeder;

import com.google.common.collect.ImmutableSet;
import com.vinted.kafka.connect.redis.RedisSinkConnectorConfig;
import com.vinted.kafka.connect.redis.RedisSinkConnectorException;
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
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class RedisStringFeeder implements IFeeder {
    private static final Logger log = LoggerFactory.getLogger(RedisStringFeeder.class);
    private final ValueConverter valueConverter;
    private final KeyConverter keyConverter;
    private final UnifiedJedis redis;
    private final RedisSinkConnectorConfig config;
    private static final Set<Object> SUCCESS_RESPONSES = ImmutableSet.of("OK", 1L, 0L);

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
        List<Object> result = null;

        try (PipelineBase pipeline = config.isRedisPipelined() ? redis.pipelined() : null) {
            SetParams params = getSetParams();

            result = collection.stream().map(record -> {
                byte[] key = keyConverter.convert(record);
                byte[] value = valueConverter.convert(record);

                if (value == null) {
                    return delete(key, pipeline);
                } else {
                    return set(key, value, params, pipeline);
                }
            }).collect(Collectors.toList());

            if (pipeline != null) {
                pipeline.sync();
            }
        }

        verifyResults(result);
    }

    private void verifyResults(List<Object> result) {
        List<Object> failures = result.stream()
                .map(RedisStringFeeder::toResponseString)
                .filter(RedisStringFeeder::isFailureResponse)
                .collect(Collectors.toList());

        if (!failures.isEmpty()) {
            throw new RedisSinkConnectorException("Failed to SET keys: " + failures);
        }
    }

    private Object set(byte[] key, byte[] value, SetParams params, PipelineBase pipeline) {
        if (pipeline != null) {
            return pipeline.set(key, value, params);
        } else {
            return redis.set(key, value, params);
        }
    }

    private Object delete(byte[] key, PipelineBase pipeline) {
        if (pipeline != null) {
            return pipeline.expire(key, 1);
        } else {
            return redis.expire(key, 1);
        }
    }

    private SetParams getSetParams() {
        SetParams params = new SetParams();

        if (config.getRedisKeyTtl() > -1) {
            params.ex(config.getRedisKeyTtl());
        }

        return params;
    }

    private static Object toResponseString(Object c) {
        if (c instanceof Response) {
            return ((Response<?>) c).get();
        } else {
            return c;
        }
    }

    private static boolean isFailureResponse(Object c) {
        return !SUCCESS_RESPONSES.contains(c);
    }
}
