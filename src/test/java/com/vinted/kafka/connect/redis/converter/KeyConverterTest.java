package com.vinted.kafka.connect.redis.converter;

import com.google.common.collect.ImmutableMap;
import com.vinted.kafka.connect.redis.RedisSinkConnectorException;
import com.vinted.kafka.connect.redis.converter.KeyConverter;
import com.vinted.kafka.connect.redis.RedisSinkConnectorConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class KeyConverterTest {

    private KeyConverter converter;

    @Test
    public void testConvertWithStringKey() {
        Map<String, Object> props = getPropsBuilder()
                .put(RedisSinkConnectorConfig.REDIS_KEY, "prefix")
                .build();
        RedisSinkConnectorConfig config = new RedisSinkConnectorConfig(props);
        converter = new KeyConverter(config);

        SinkRecord record = new SinkRecord("", 0, null, "foo",
                Schema.STRING_SCHEMA, "bar", 0);

        byte[] converted = converter.convert(record);

        assertEquals("prefix:foo", new String(converted));
    }

    @Test
    public void testConvertWithIntKey() {
        Map<String, Object> props = getPropsBuilder().build();
        RedisSinkConnectorConfig config = new RedisSinkConnectorConfig(props);

        converter = new KeyConverter(config);

        SinkRecord record = new SinkRecord("", 0, null, 42,
                Schema.INT32_SCHEMA, "bar", 0);

        byte[] converted = converter.convert(record);

        assertEquals("42", new String(converted));
    }

    @Test
    public void testConvertWithUnsupportedKey() {
        Map<String, Object> props = getPropsBuilder().build();
        RedisSinkConnectorConfig config = new RedisSinkConnectorConfig(props);

        converter = new KeyConverter(config);

        SinkRecord record = new SinkRecord("", 0, null, new Object(),
                Schema.FLOAT64_SCHEMA, "bar", 0);

        assertThrows(RedisSinkConnectorException.class, () -> converter.convert(record));
    }

    // Tests for null key, empty key
    // Tests for other supported types

    private static ImmutableMap.Builder<String, Object> getPropsBuilder() {
        return ImmutableMap.<String, Object>builder()
                .put(RedisSinkConnectorConfig.REDIS_URI, "some_uri");
    }
}
