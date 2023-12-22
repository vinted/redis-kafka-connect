package com.vinted.kafka.connect.redis.converter;

import org.junit.jupiter.api.Test;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.ImmutableArrayValue;
import org.msgpack.value.NumberValue;
import org.msgpack.value.Value;
import org.msgpack.value.impl.ImmutableArrayValueImpl;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class ValueConverterTest {

    private ValueConverter converter = new ValueConverter();

    @Test
    public void testConvertWithByteArrayValue() {
        byte[] value = "Hello".getBytes();
        SinkRecord record = new SinkRecord("", 0, null, null, Schema.BYTES_SCHEMA, value, 0);

        String converted = new String(converter.convert(record), StandardCharsets.UTF_8);

        assertEquals("Hello", converted);
    }

    @Test
    public void testConvertWithStringValue() {
        String value = "world";
        SinkRecord record = new SinkRecord("", 0, null, null, Schema.STRING_SCHEMA, value, 0);

        String converted = new String(converter.convert(record), StandardCharsets.UTF_8);

        assertEquals("world", converted);
    }

    @Test
    public void testConvertWithMessagePackedValue() throws IOException {
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
        packer.packArrayHeader(4)
                .packInt(123456)
                .packString("abc def")
                .packDouble(1.55)
                .packDouble(0.9)
                .close();

        byte[] value = packer.toByteArray();
        SinkRecord record = new SinkRecord("", 0, null, null, Schema.BYTES_SCHEMA, value, 0);
        byte[] convertedValue = converter.convert(record);

        assertEquals(value, convertedValue);

        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(convertedValue);
        List<Value> values = unpacker.unpackValue().asArrayValue().list();
        assertEquals(4, values.size());
    }
}
