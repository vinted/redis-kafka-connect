package com.vinted.kafka.connect.redis.feeder;

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Collection;

public interface IFeeder {
    void feed(Collection<SinkRecord> collection);
}
