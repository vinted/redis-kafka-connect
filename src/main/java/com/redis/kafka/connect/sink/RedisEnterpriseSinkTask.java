/*
 * Copyright © 2021 Redis
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.redis.kafka.connect.sink;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.jcustenborder.kafka.connect.utils.data.SinkOffsetState;
import com.github.jcustenborder.kafka.connect.utils.jackson.ObjectMapperFactory;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.redis.kafka.connect.RedisEnterpriseSinkConnector;
import com.redis.lettucemod.RedisModulesClient;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.RedisItemWriter.OperationItemWriterBuilder;
import com.redis.spring.batch.RedisItemWriter.RedisItemWriterBuilder;
import com.redis.spring.batch.support.RedisOperation;
import com.redis.spring.batch.support.convert.SampleConverter;
import com.redis.spring.batch.support.convert.ScoredValueConverter;
import com.redis.spring.batch.support.operation.Hset;
import com.redis.spring.batch.support.operation.JsonSet;
import com.redis.spring.batch.support.operation.Lpush;
import com.redis.spring.batch.support.operation.Rpush;
import com.redis.spring.batch.support.operation.Sadd;
import com.redis.spring.batch.support.operation.Set;
import com.redis.spring.batch.support.operation.TsAdd;
import com.redis.spring.batch.support.operation.Xadd;
import com.redis.spring.batch.support.operation.Zadd;

import io.lettuce.core.KeyValue;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;

public class RedisEnterpriseSinkTask extends SinkTask {

	private static final Logger log = LoggerFactory.getLogger(RedisEnterpriseSinkTask.class);
	private static final String OFFSET_KEY_FORMAT = "com.redis.kafka.connect.sink.offset.%s.%s";

	private RedisModulesClient client;
	private RedisEnterpriseSinkConfig config;
	private Charset charset;
	private RedisItemWriter<byte[], byte[], SinkRecord> writer;
	private StatefulRedisConnection<String, String> connection;

	@Override
	public String version() {
		return new RedisEnterpriseSinkConnector().version();
	}

	@Override
	public void start(final Map<String, String> props) {
		config = new RedisEnterpriseSinkConfig(props);
		client = RedisModulesClient.create(config.getRedisURI());
		connection = client.connect();
		charset = config.getCharset();
		writer = writer(client).build();
		writer.open(new ExecutionContext());
		final java.util.Set<TopicPartition> assignment = this.context.assignment();
		if (!assignment.isEmpty()) {
			Map<TopicPartition, Long> partitionOffsets = new HashMap<>(assignment.size());
			for (SinkOffsetState state : offsetStates(assignment)) {
				partitionOffsets.put(state.topicPartition(), state.offset());
				log.info("Requesting offset {} for {}", state.offset(), state.topicPartition());
			}
			for (TopicPartition topicPartition : assignment) {
				partitionOffsets.putIfAbsent(topicPartition, 0L);
			}
			this.context.offset(partitionOffsets);
		}
	}

	private Collection<SinkOffsetState> offsetStates(java.util.Set<TopicPartition> assignment) {
		Collection<SinkOffsetState> offsetStates = new ArrayList<>();
		String[] partitionKeys = assignment.stream().map(a -> offsetKey(a.topic(), a.partition()))
				.toArray(String[]::new);
		List<KeyValue<String, String>> values = connection.sync().mget(partitionKeys);
		for (KeyValue<String, String> value : values) {
			if (value.hasValue()) {
				try {
					offsetStates.add(ObjectMapperFactory.INSTANCE.readValue(value.getValue(), SinkOffsetState.class));
				} catch (IOException e) {
					throw new DataException(e);
				}
			}
		}
		return offsetStates;
	}

	private RedisItemWriterBuilder<byte[], byte[], SinkRecord> writer(RedisModulesClient client) {
		RedisItemWriterBuilder<byte[], byte[], SinkRecord> builder = new OperationItemWriterBuilder<>(client,
				new ByteArrayCodec()).operation(operation());
		if (Boolean.TRUE.equals(config.isMultiexec())) {
			builder.multiExec();
		}
		if (config.getWaitReplicas() > 0) {
			builder.waitForReplication(config.getWaitReplicas(), config.getWaitTimeout());
		}
		return builder;
	}

	private String offsetKey(String topic, Integer partition) {
		return String.format(OFFSET_KEY_FORMAT, topic, partition);
	}

	private RedisOperation<byte[], byte[], SinkRecord> operation() {
		switch (config.getType()) {
		case STREAM:
			return Xadd.<byte[], byte[], SinkRecord>key(this::collectionKey).body(this::map).build();
		case HASH:
			return Hset.<byte[], byte[], SinkRecord>key(this::key).map(this::map).del(this::isDelete).build();
		case JSON:
			return JsonSet.<byte[], byte[], SinkRecord>key(this::key).path(".".getBytes(charset)).value(this::value)
					.del(this::isDelete).build();
		case STRING:
			return Set.<byte[], byte[], SinkRecord>key(this::key).value(this::value).del(this::isDelete).build();
		case LIST:
			if (config.getPushDirection() == RedisEnterpriseSinkConfig.PushDirection.LEFT) {
				return Lpush.<byte[], byte[], SinkRecord>key(this::collectionKey).member(this::key).build();
			}
			return Rpush.<byte[], byte[], SinkRecord>key(this::collectionKey).member(this::key).build();
		case SET:
			return Sadd.<byte[], byte[], SinkRecord>key(this::collectionKey).member(this::key).build();
		case TIMESERIES:
			return TsAdd.<byte[], byte[], SinkRecord>key(this::collectionKey)
					.sample(new SampleConverter<>(this::longKey, this::doubleValue)).build();
		case ZSET:
			return Zadd.<byte[], byte[], SinkRecord>key(this::collectionKey)
					.value(new ScoredValueConverter<>(this::key, this::doubleValue)).build();
		default:
			throw new ConfigException(RedisEnterpriseSinkConfig.TYPE_CONFIG, config.getType());
		}
	}

	private byte[] value(SinkRecord sinkRecord) {
		return bytes("value", sinkRecord.value());
	}

	private Long longKey(SinkRecord sinkRecord) {
		Object key = sinkRecord.key();
		if (key == null) {
			return null;
		}
		if (key instanceof Number) {
			return ((Number) key).longValue();
		}
		throw new DataException(
				"The key for the record must be a number. Consider using a single message transformation to transform the data before it is written to Redis.");
	}

	private Double doubleValue(SinkRecord sinkRecord) {
		Object value = sinkRecord.value();
		if (value == null) {
			return null;
		}
		if (value instanceof Number) {
			return ((Number) value).doubleValue();
		}
		throw new DataException(
				"The value for the record must be a number. Consider using a single message transformation to transform the data before it is written to Redis.");
	}

	private boolean isDelete(SinkRecord sinkRecord) {
		return sinkRecord.value() == null;
	}

	private byte[] key(SinkRecord sinkRecord) {
		return bytes("key", sinkRecord.key());
	}

	private byte[] bytes(String source, Object input) {
		if (input == null) {
			return null;
		}
		if (input instanceof byte[]) {
			return (byte[]) input;
		}
		if (input instanceof String) {
			return ((String) input).getBytes(charset);
		}
		throw new DataException(String.format(
				"The %s for the record must be a string or byte array. Consider using the StringConverter or ByteArrayConverter if the data is stored in Kafka in the format needed in Redis.",
				source));
	}

	private byte[] collectionKey(SinkRecord sinkRecord) {
		return config.getKeyFormat().replace(RedisEnterpriseSinkConfig.TOKEN_TOPIC, sinkRecord.topic())
				.getBytes(charset);
	}

	@SuppressWarnings("unchecked")
	private Map<byte[], byte[]> map(SinkRecord sinkRecord) {
		Object value = sinkRecord.value();
		if (value == null) {
			return null;
		}
		if (value instanceof Struct) {
			Map<byte[], byte[]> body = new LinkedHashMap<>();
			Struct struct = (Struct) value;
			for (Field field : struct.schema().fields()) {
				Object fieldValue = struct.get(field);
				body.put(field.name().getBytes(charset),
						fieldValue == null ? null : fieldValue.toString().getBytes(charset));
			}
			return body;
		}
		if (value instanceof Map) {
			Map<String, Object> map = (Map<String, Object>) value;
			Map<byte[], byte[]> body = new LinkedHashMap<>();
			for (Map.Entry<String, Object> e : map.entrySet()) {
				body.put(e.getKey().getBytes(charset), String.valueOf(e.getValue()).getBytes(charset));
			}
			return body;
		}
		throw new ConnectException("Unsupported source value type: " + sinkRecord.valueSchema().type().name());
	}

	@Override
	public void stop() {
		if (writer != null) {
			writer.close();
		}
		if (connection != null) {
			connection.close();
		}
		if (client != null) {
			client.shutdown();
			client.getResources().shutdown();
		}
	}

	@Override
	public void put(final Collection<SinkRecord> records) {
		log.debug("Processing {} records", records.size());
		try {
			writer.write(new ArrayList<>(records));
			log.info("Wrote {} records", records.size());
		} catch (Exception e) {
			log.warn("Could not write {} records", records.size(), e);
		}
		Map<TopicPartition, Long> data = new ConcurrentHashMap<>(100);
		for (SinkRecord sinkRecord : records) {
			Preconditions.checkState(!Strings.isNullOrEmpty(sinkRecord.topic()), "topic cannot be null or empty.");
			Preconditions.checkNotNull(sinkRecord.kafkaPartition(), "partition cannot be null.");
			Preconditions.checkState(sinkRecord.kafkaOffset() >= 0, "offset must be greater than or equal 0.");
			TopicPartition partition = new TopicPartition(sinkRecord.topic(), sinkRecord.kafkaPartition());
			long current = data.getOrDefault(partition, Long.MIN_VALUE);
			if (sinkRecord.kafkaOffset() > current) {
				data.put(partition, sinkRecord.kafkaOffset());
			}
		}
		List<SinkOffsetState> offsetData = data.entrySet().stream()
				.map(e -> SinkOffsetState.of(e.getKey(), e.getValue())).collect(Collectors.toList());
		if (!offsetData.isEmpty()) {
			Map<String, String> offsets = new LinkedHashMap<>(offsetData.size());
			for (SinkOffsetState e : offsetData) {
				String key = offsetKey(e.topic(), e.partition());
				String value;
				try {
					value = ObjectMapperFactory.INSTANCE.writeValueAsString(e);
				} catch (JsonProcessingException e1) {
					throw new DataException(e1);
				}
				offsets.put(key, value);
				log.trace("put() - Setting offset: {}", e);
			}
			connection.sync().mset(offsets);
		}
	}

}