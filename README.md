# vinted-redis-kafka-connect

A simple Redis Kafka connector. 

## Sample config

```json
{
    "name": "redis-cluster-sink4",
    "config": {
        "connector.class": "com.vinted.kafka.connect.redis.RedisSinkConnector",
        "tasks.max": "5",
        "topics": "pageviews",
        "redis.uri": "redis-cluster:7000;redis-cluster:7001;redis-cluster:7002",
        "redis.cluster": true,
        "redis.pipelined": true,
        "redis.type": "string",
        "redis.key": "keyspace",
        "redis.key.ttl": "300",
        "redis.socket.connect.timeout.ms": 10000,
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false"
    }
}
```
