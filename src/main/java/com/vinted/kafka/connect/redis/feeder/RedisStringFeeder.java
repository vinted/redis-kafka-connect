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

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

            Stream<Object> result = collection.stream().map(record -> {
                byte[] key = keyConverter.convert(record);
                byte[] value = valueConverter.convert(record);

                if (value == null) {
                    System.out.println("DEBUG EX (cluster: " + config.isRedisCluster() + ") : " + new String(key, StandardCharsets.UTF_8));
                    return delete(key, pipeline);
                } else {
                    System.out.println("DEBUG (cluster: " + config.isRedisCluster() + ") : " + new String(key, StandardCharsets.UTF_8) + " : " + new String(value, StandardCharsets.UTF_8));
                    return set(key, value, params, pipeline);
                }
            });
            
            if (pipeline!= null) {
                pipeline.sync();
            }
            verifyResults(result.collect(Collectors.toList()));
        }
    }

    private void verifyResults(List<Object> result) {
        // TODO: verify results
        System.out.println(result.stream().map(Object::toString).collect(Collectors.toList()));
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
}
