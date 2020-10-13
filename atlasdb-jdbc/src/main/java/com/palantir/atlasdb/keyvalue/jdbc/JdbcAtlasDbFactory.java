/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.jdbc;

import java.util.Optional;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.config.DbTimestampCreationSetting;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.spi.KeyValueServiceRuntimeConfig;
import com.palantir.atlasdb.timestamp.TimestampCreationParametersCheck;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.versions.AtlasDbVersion;
import com.palantir.timestamp.ManagedTimestampService;
import com.palantir.timestamp.PersistentTimestampServiceImpl;

@AutoService(AtlasDbFactory.class)
public class JdbcAtlasDbFactory implements AtlasDbFactory {
    private static final Logger log = LoggerFactory.getLogger(JdbcAtlasDbFactory.class);

    @Override
    public String getType() {
        return JdbcKeyValueConfiguration.TYPE;
    }

    /**
     * Creates a JdbcKeyValueService.
     *
     * @param config Configuration file.
     * @param runtimeConfig unused.
     * @param leaderConfig unused.
     * @param unused unused.
     * @param unusedLongSupplier unused.
     * @param initializeAsync unused. Async initialization has not been implemented and is not propagated.
     * @return The requested KeyValueService instance
     */
    @Override
    public KeyValueService createRawKeyValueService(
            MetricsManager metricsManager,
            KeyValueServiceConfig config,
            Supplier<Optional<KeyValueServiceRuntimeConfig>> runtimeConfig,
            Optional<LeaderConfig> leaderConfig,
            Optional<String> unused,
            LongSupplier unusedLongSupplier,
            boolean initializeAsync) {
        if (initializeAsync) {
            log.warn("Asynchronous initialization not implemented, will initialize synchronousy.");
        }

        AtlasDbVersion.ensureVersionReported();
        return JdbcKeyValueService.create((JdbcKeyValueConfiguration) config);
    }

    @Override
    public ManagedTimestampService createManagedTimestampService(
            KeyValueService rawKvs,
            Optional<DbTimestampCreationSetting> creationParameters,
            boolean initializeAsync) {
        if (initializeAsync) {
            log.warn("Asynchronous initialization not implemented, will initialize synchronously.");
        }

        Preconditions.checkArgument(
                TimestampCreationParametersCheck.areCreationParametersConsistentWithDefaults(creationParameters),
                "***ERROR:This can cause severe data corruption.***\nUnexpected timestamp params found: "
                        + creationParameters
                        + "\nThis can happen if you configure the timelock server to use JDBC KVS for timestamp"
                        + " persistence, which is unsupported.\nWe recommend using the default paxos timestamp"
                        + " persistence. However, if you are need to persist the timestamp service state in the"
                        + " database, please specify a valid DbKvs config in the timestampBoundPersister block."
                        + "\nNote that if the service has already been running, you will have to migrate the timestamp"
                        + " table to Postgres/Oracle: please contact support. DO NOT TRY TO FIX THIS YOURSELF.");

        AtlasDbVersion.ensureVersionReported();
        return PersistentTimestampServiceImpl.create(JdbcTimestampBoundStore.create((JdbcKeyValueService) rawKvs));
    }
}
