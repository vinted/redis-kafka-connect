package com.vinted.kafka.connect.redis.feeder;

import com.vinted.kafka.connect.redis.converter.KeyConverter;
import com.vinted.kafka.connect.redis.converter.ValueConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import redis.clients.jedis.PipelineBase;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.params.SetParams;

import java.util.Collection;
import java.util.Map;

public class RedisStringFeeder implements IFeeder {
    private final Map<String, String> props;
    private final ValueConverter valueConverter;
    private final KeyConverter keyConverter;
    private final UnifiedJedis redis;

    public RedisStringFeeder(UnifiedJedis redis, Map<String, String> props) {
        this.props = props;
        this.valueConverter = new ValueConverter();
        this.keyConverter = new KeyConverter();
        this.redis = redis;
    }

    @Override
    public void feed(Collection<SinkRecord> collection) {
        PipelineBase pipeline = redis.pipelined();
        collection.forEach(record -> set(pipeline, record));
    }

    private void set(PipelineBase redis, SinkRecord record) {
        String key = keyConverter.convert(record);
        String value = valueConverter.convert(record);
        SetParams params = getSetParams();
        redis.set(key, value, params);
    }

    private SetParams getSetParams() {
        SetParams params = new SetParams();
        // TODO : add expiration
        return params;
    }
}
