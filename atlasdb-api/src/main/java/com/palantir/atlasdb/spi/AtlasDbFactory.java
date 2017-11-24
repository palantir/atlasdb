/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
 * ​
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * ​
 * http://opensource.org/licenses/BSD-3-Clause
 * ​
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.spi;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.qos.FakeQosClient;
import com.palantir.atlasdb.qos.QosClient;
import com.palantir.timestamp.TimestampService;
import com.palantir.timestamp.TimestampStoreInvalidator;

public interface AtlasDbFactory {
    long NO_OP_FAST_FORWARD_TIMESTAMP = Long.MIN_VALUE + 1; // Note: Long.MIN_VALUE itself is not allowed.
    boolean DEFAULT_INITIALIZE_ASYNC = false;

    String getType();

    default KeyValueService createRawKeyValueService(
            KeyValueServiceConfig config, Optional<LeaderConfig> leaderConfig) {
        return createRawKeyValueService(config, leaderConfig, Optional.empty(), DEFAULT_INITIALIZE_ASYNC,
                FakeQosClient.INSTANCE);
    }

    /**
     * Creates a KeyValueService instance of type according to the config parameter.
     *
     * @param config Configuration file.
     * @param leaderConfig If the implementation supports it, the optional leader configuration.
     * @param namespace If the implementation supports it, this is the namespace to use when the namespace in config is
     * absent. If both are present, they must match.
     * @param initializeAsync If the implementations supports it, and initializeAsync is true, the KVS will initialize
     * asynchronously when synchronous initialization fails.
     * @param qosClient the client for checking limits from the Quality-of-Service service.
     * @return The requested KeyValueService instance
     */
    KeyValueService createRawKeyValueService(
            KeyValueServiceConfig config,
            Optional<LeaderConfig> leaderConfig,
            Optional<String> namespace,
            boolean initializeAsync,
            QosClient qosClient);

    default TimestampService createTimestampService(KeyValueService rawKvs) {
        return createTimestampService(rawKvs, Optional.empty(), DEFAULT_INITIALIZE_ASYNC);
    }

    TimestampService createTimestampService(
            KeyValueService rawKvs,
            Optional<TableReference> timestampTable,
            boolean initializeAsync);

    default TimestampStoreInvalidator createTimestampStoreInvalidator(KeyValueService rawKvs) {
        return () -> {
            Logger log = LoggerFactory.getLogger(AtlasDbFactory.class);
            log.warn("AtlasDB doesn't yet support automated migration for KVS type {}.", getType());
            return NO_OP_FAST_FORWARD_TIMESTAMP;
        };
    }
}
