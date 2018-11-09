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
package com.palantir.atlasdb.spi;

import java.util.Optional;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampService;
import com.palantir.timestamp.TimestampStoreInvalidator;

public interface AtlasDbFactory {
    Logger log = LoggerFactory.getLogger(AtlasDbFactory.class);

    long NO_OP_FAST_FORWARD_TIMESTAMP = Long.MIN_VALUE + 1; // Note: Long.MIN_VALUE itself is not allowed.
    boolean DEFAULT_INITIALIZE_ASYNC = false;
    LongSupplier THROWING_FRESH_TIMESTAMP_SOURCE = () -> {
        throw new UnsupportedOperationException("Not expecting to use fresh timestamps");
    };

    String getType();

    default KeyValueService createRawKeyValueService(
            MetricsManager metricsManager, KeyValueServiceConfig config, Optional<LeaderConfig> leaderConfig) {
        return createRawKeyValueService(
                metricsManager,
                config,
                Optional::empty,
                leaderConfig,
                Optional.empty(),
                THROWING_FRESH_TIMESTAMP_SOURCE,
                DEFAULT_INITIALIZE_ASYNC);
    }

    /**
     * Creates a KeyValueService instance of type according to the config parameter.
     *
     * @param config Configuration file.
     * @param runtimeConfig Runtime configuration file.
     * @param leaderConfig If the implementation supports it, the optional leader configuration.
     * @param namespace If the implementation supports it, this is the namespace to use when the namespace in config is
     * absent. If both are present, they must match.
     * @param freshTimestampSource If present, a source of fresh timestamps, which may be relevant for some KVS
     * operations.
     * @param initializeAsync If the implementations supports it, and initializeAsync is true, the KVS will initialize
     * asynchronously when synchronous initialization fails.
     * @return The requested KeyValueService instance
     */
    KeyValueService createRawKeyValueService(
            MetricsManager metricsManager,
            KeyValueServiceConfig config,
            Supplier<Optional<KeyValueServiceRuntimeConfig>> runtimeConfig,
            Optional<LeaderConfig> leaderConfig,
            Optional<String> namespace,
            LongSupplier freshTimestampSource,
            boolean initializeAsync);

    default TimestampService createTimestampService(KeyValueService rawKvs) {
        return createTimestampService(rawKvs, Optional.empty(), DEFAULT_INITIALIZE_ASYNC);
    }

    TimestampService createTimestampService(
            KeyValueService rawKvs,
            Optional<TableReference> timestampTable,
            boolean initializeAsync);

    static TimestampManagementService createTimestampManagementService(TimestampService timestampService) {
        Preconditions.checkArgument(timestampService instanceof TimestampManagementService,
                "TimestampManagementService must be created based on result of createTimestampService call."
                        + "\nExpected a TS implementing TimestampManagementService, got %s",
                timestampService.getClass());

        return (TimestampManagementService) timestampService;

    }

    default TimestampStoreInvalidator createTimestampStoreInvalidator(KeyValueService rawKvs) {
        return () -> {
            log.warn("AtlasDB doesn't yet support automated migration for KVS type {}.", getType());
            return NO_OP_FAST_FORWARD_TIMESTAMP;
        };
    }
}
