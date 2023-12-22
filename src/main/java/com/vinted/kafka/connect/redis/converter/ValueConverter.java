package com.vinted.kafka.connect.redis.converter;

import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;

import java.nio.charset.StandardCharsets;
import java.util.Collections;

public class ValueConverter {
    private static final Converter JSON_CONVERTER;

    static {
        JSON_CONVERTER = new JsonConverter();
        JSON_CONVERTER.configure(Collections.singletonMap("schemas.enable", "false"), false);
    }

    public byte[] convert(SinkRecord record) {

        if (record.value() == null) {
            return null;
        } else if (record.value() instanceof byte[]) {
            return (byte[]) record.value();
        } else if (record.value() instanceof String) {
            return ((String) record.value()).getBytes(StandardCharsets.UTF_8);
        } else {
            return JSON_CONVERTER.fromConnectData(record.topic(), record.valueSchema(), record.value());
        }
    }
}
