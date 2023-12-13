package com.vinted.kafka.connect.redis;

import com.vinted.kafka.connect.redis.feeder.IFeeder;
import com.vinted.kafka.connect.redis.feeder.RedisStringFeeder;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class RedisSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(RedisSinkTask.class);
    private UnifiedJedis redis;
    private IFeeder feeder;
    private RedisSinkConnectorConfig config;

    @Override
    public String version() {
        return RedisSinkConnectorVersion.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        this.config = new RedisSinkConnectorConfig(props);
        this.redis = initRedis();
        this.feeder = initFeeder();
    }

    @Override
    public void stop() {
        this.redis.close();
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        if (collection.isEmpty()) {
            return;
        }

        log.debug("Putting {} records.", collection.size());

        try {
            feeder.feed(collection);
        } catch (Exception e) {
            log.error("Error feeding records to Redis.", e);
            throw new RedisSinkConnectorException("Error feeding records to Redis", e);
        }
    }

    private UnifiedJedis initRedis() {
        if (config.isRedisCluster()) {
            Set<HostAndPort> clusterNodes = Arrays.stream(config.getRedisUri().split(";"))
                    .map(c -> c.split(":"))
                    .map(c -> new HostAndPort(c[0], Integer.parseInt(c[1])))
                    .collect(Collectors.toSet());
            return new JedisCluster(clusterNodes, config.getRedisTimeout());
        } else {
            String[] redisUriSplit = config.getRedisUri().split(":");
            String host = redisUriSplit[0];
            int port = Integer.parseInt(redisUriSplit[1]);
            return new JedisPooled(host, port);
        }
    }

    private IFeeder initFeeder() {
        if ("string".equals(config.getRedisType())) {
            return new RedisStringFeeder(redis, config);
        }
        throw new RedisSinkConnectorException("Unsupported redis sink type: " + config.getRedisType());
    }
}
