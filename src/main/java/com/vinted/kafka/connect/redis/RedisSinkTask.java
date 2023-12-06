package com.vinted.kafka.connect.redis;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.UnifiedJedis;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

public class RedisSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(RedisSinkTask.class);
    private String redisUri;
    private boolean isRedisCluster;
    private String redisType;
    private String redisKey;
    private int redisTimeout;
    private UnifiedJedis redis;

    @Override
    public String version() {
        return RedisSinkConnectorVersion.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        this.redisUri = props.get(RedisSinkConnectorConfig.REDIS_URI);
        this.isRedisCluster = "true".equals(props.get(RedisSinkConnectorConfig.REDIS_CLUSTER));
        this.redisType = props.get(RedisSinkConnectorConfig.REDIS_TYPE);
        this.redisKey = props.get(RedisSinkConnectorConfig.REDIS_KEY);
        this.redisTimeout = Integer.parseInt(props.get(RedisSinkConnectorConfig.REDIS_CONNECT_TIMEOUT));
        this.redis = initRedis(redisUri, isRedisCluster);
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
//            feeder.feed(collection);
        } catch (Exception e) {
            log.error("Error feeding records to Redis.", e);
            throw new ConnectException(e);
        }
    }

    private UnifiedJedis initRedis(String redisUri, boolean isRedisCluster) {
        if (isRedisCluster) {
            var clusterNodes = Arrays.stream(this.redisUri.split(";"))
                    .map(c -> c.split(":"))
                    .map(c -> new HostAndPort(c[0], Integer.parseInt(c[1])))
                    .collect(Collectors.toSet());
            return new JedisCluster(clusterNodes);
        } else {
            var redisUriSplit = this.redisUri.split(":");
            return new JedisPooled(redisUriSplit[0], Integer.parseInt(redisUriSplit[1]));
        }
    }
}
