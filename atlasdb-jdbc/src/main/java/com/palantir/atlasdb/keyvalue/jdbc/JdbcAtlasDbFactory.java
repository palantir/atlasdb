/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.jdbc;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auto.service.AutoService;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.versions.AtlasDbVersion;
import com.palantir.timestamp.PersistentTimestampServiceImpl;
import com.palantir.timestamp.TimestampService;

@AutoService(AtlasDbFactory.class)
public class JdbcAtlasDbFactory implements AtlasDbFactory {
    private static final Logger log = LoggerFactory.getLogger(JdbcAtlasDbFactory.class);

    @Override
    public String getType() {
        return JdbcKeyValueConfiguration.TYPE;
    }

    // async initialization not implemented/propagated
    @Override
    public KeyValueService createRawKeyValueService(KeyValueServiceConfig config, Optional<LeaderConfig> leaderConfig,
            boolean initializeAsync) {
        if (initializeAsync) {
            log.warn("Asynchronous initialization not implemented, will initialize synchronousy.");
        }
        AtlasDbVersion.ensureVersionReported();
        return JdbcKeyValueService.create((JdbcKeyValueConfiguration) config);
    }

    @Override
    public TimestampService createTimestampService(KeyValueService rawKvs, boolean initializeAsync) {
        AtlasDbVersion.ensureVersionReported();
        return PersistentTimestampServiceImpl.create(JdbcTimestampBoundStore.create((JdbcKeyValueService) rawKvs));
    }
}
