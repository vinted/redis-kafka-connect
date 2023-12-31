package com.vinted.kafka.connect.redis.feeder;

import static org.junit.jupiter.api.Assertions.*;

import com.google.common.collect.ImmutableMap;
import com.vinted.kafka.connect.redis.feeder.RedisStringFeeder;
import com.vinted.kafka.connect.redis.RedisSinkConnectorConfig;
import com.vinted.kafka.connect.redis.converter.KeyConverter;
import com.vinted.kafka.connect.redis.converter.ValueConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mockito;
import org.junit.jupiter.api.Test;

import redis.clients.jedis.PipelineBase;
import redis.clients.jedis.Response;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.params.SetParams;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

import static org.mockito.Mockito.*;

public class RedisStringFeederTest {

    private UnifiedJedis jedis;
    private PipelineBase pipeline;
    private KeyConverter keyConverter;
    private ValueConverter valueConverter;
    private byte[] convertedKeyBytes;
    private byte[] convertedValueBytes;
    private Response response;
    private Response responseExpire;

    @BeforeEach
    public void setUp() {
        this.jedis = mock(UnifiedJedis.class);
        this.keyConverter = mock(KeyConverter.class);
        this.valueConverter = mock(ValueConverter.class);
        this.pipeline = mock(PipelineBase.class);
        this.convertedKeyBytes = "convertedKey".getBytes(StandardCharsets.UTF_8);
        this.convertedValueBytes = "convertedValue".getBytes(StandardCharsets.UTF_8);
        this.response = mock(Response.class);
        this.responseExpire = mock(Response.class);
    }

    @Test
    public void testFeedWithSet() {
        SinkRecord record = new SinkRecord("", 0, null, "key", null, "value", 0);

        Map<String, Object> props = getPropsBuilder().build();
        RedisSinkConnectorConfig config = new RedisSinkConnectorConfig(props);

        when(keyConverter.convert(record)).thenReturn(convertedKeyBytes);
        when(valueConverter.convert(record)).thenReturn(convertedValueBytes);
        when(jedis.set(any(), (byte[]) any(), any())).thenReturn("OK");

        RedisStringFeeder feeder = new RedisStringFeeder(jedis, config, keyConverter, valueConverter);

        feeder.feed(Collections.singletonList(record));

        verify(jedis).set(eq(convertedKeyBytes), eq(convertedValueBytes), any(SetParams.class));;
    }

    @Test
    public void testFeedWithExpire() {
        SinkRecord record = new SinkRecord("", 0, null, "key", null, null, 0);

        Map<String, Object> props = getPropsBuilder().build();
        RedisSinkConnectorConfig config = new RedisSinkConnectorConfig(props);

        when(keyConverter.convert(record)).thenReturn(convertedKeyBytes);
        when(valueConverter.convert(record)).thenReturn(null);
        when(jedis.expire((byte[]) any(), eq(1))).thenReturn(1L);

        RedisStringFeeder feeder = new RedisStringFeeder(jedis, config, keyConverter, valueConverter);

        feeder.feed(Collections.singletonList(record));

        verify(jedis).expire(eq(convertedKeyBytes), eq(1L));;
    }

    @Test
    public void testFeedWithPipeline() {
        SinkRecord record = new SinkRecord("", 0, null, "key", null, "value", 0);

        Map<String, Object> props = getPropsBuilder()
                .put(RedisSinkConnectorConfig.REDIS_PIPELINED, true)
                .build();
        RedisSinkConnectorConfig config = new RedisSinkConnectorConfig(props);

        when(jedis.pipelined()).thenReturn(pipeline);
        when(keyConverter.convert(record)).thenReturn(convertedKeyBytes);
        when(valueConverter.convert(record)).thenReturn(convertedValueBytes);
        when(response.get()).thenReturn("OK");
        when(pipeline.set(any(), (byte[]) any(), any())).thenReturn(this.response);

        RedisStringFeeder feeder = new RedisStringFeeder(jedis, config, keyConverter, valueConverter);

        feeder.feed(Collections.singletonList(record));

        verify(pipeline).set(eq(convertedKeyBytes), eq(convertedValueBytes), any(SetParams.class));;
    }

    @Test
    public void testFeedExpireWithPipeline() {
        SinkRecord record = new SinkRecord("", 0, null, "key", null, null, 0);

        Map<String, Object> props = getPropsBuilder()
                .put(RedisSinkConnectorConfig.REDIS_PIPELINED, true)
                .build();
        RedisSinkConnectorConfig config = new RedisSinkConnectorConfig(props);

        when(jedis.pipelined()).thenReturn(pipeline);
        when(keyConverter.convert(record)).thenReturn(convertedKeyBytes);
        when(valueConverter.convert(record)).thenReturn(null);
        when(responseExpire.get()).thenReturn(0L);
        when(pipeline.expire((byte[]) any(), eq(1L))).thenReturn(this.responseExpire);

        RedisStringFeeder feeder = new RedisStringFeeder(jedis, config, keyConverter, valueConverter);

        feeder.feed(Collections.singletonList(record));

        verify(pipeline).expire(eq(convertedKeyBytes), eq(1L));;
    }

    private static ImmutableMap.Builder<String, Object> getPropsBuilder() {
        return ImmutableMap.<String, Object>builder()
                .put(RedisSinkConnectorConfig.REDIS_URI, "some_uri");
    }
}
