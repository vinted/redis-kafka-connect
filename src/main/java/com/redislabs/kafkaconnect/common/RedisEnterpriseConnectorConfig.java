/*
 * Copyright © 2021 Redis Labs
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
package com.redislabs.kafkaconnect.common;

import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;
import com.github.jcustenborder.kafka.connect.utils.config.validators.Validators;
import lombok.Getter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class RedisEnterpriseConnectorConfig extends AbstractConfig {

    public static final String REDIS_URI = "redis.uri";
    private static final String REDIS_URI_DEFAULT = "redis://localhost:6379";
    private static final String REDIS_URI_DOC = "URI of the Redis Enterprise database to connect to, e.g. redis://redis-12000.redislabs.com:12000";

    @Getter
    private final String redisUri;

    public RedisEnterpriseConnectorConfig(ConfigDef config, Map<?, ?> originals) {
        super(config, originals);
        redisUri = getString(REDIS_URI);
    }

    protected static ConfigDef config() {
        return new ConfigDef()
                .define(ConfigKeyBuilder.of(REDIS_URI, ConfigDef.Type.STRING).documentation(REDIS_URI_DOC).defaultValue(REDIS_URI_DEFAULT).importance(ConfigDef.Importance.HIGH).validator(Validators.validURI("redis", "rediss")).build());
    }
}