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
import com.palantir.timestamp.TimestampService;
import com.palantir.timestamp.TimestampStoreInvalidator;

public interface AtlasDbFactory {
    long NO_OP_FAST_FORWARD_TIMESTAMP = Long.MIN_VALUE + 1; // Note: Long.MIN_VALUE itself is not allowed.

    String getType();

    default KeyValueService createRawKeyValueService(
            KeyValueServiceConfig config, Optional<LeaderConfig> leaderConfig) {
        return createRawKeyValueService(config, leaderConfig, Optional.empty());
    }

    KeyValueService createRawKeyValueService(
            KeyValueServiceConfig config, Optional<LeaderConfig> leaderConfig, Optional<String> namespace);

    TimestampService createTimestampService(KeyValueService rawKvs);

    default TimestampStoreInvalidator createTimestampStoreInvalidator(KeyValueService rawKvs) {
        return () -> {
            Logger log = LoggerFactory.getLogger(AtlasDbFactory.class);
            log.warn("AtlasDB doesn't yet support automated migration for KVS type {}.", getType());
            return NO_OP_FAST_FORWARD_TIMESTAMP;
        };
    }
}
