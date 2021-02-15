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
package com.palantir.atlasdb.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.compact.CompactorConfig;
import com.palantir.atlasdb.internalschema.ImmutableInternalSchemaConfig;
import com.palantir.atlasdb.internalschema.InternalSchemaConfig;
import com.palantir.atlasdb.spi.KeyValueServiceRuntimeConfig;
import com.palantir.atlasdb.stream.StreamStorePersistenceConfiguration;
import com.palantir.atlasdb.stream.StreamStorePersistenceConfigurations;
import com.palantir.atlasdb.sweep.queue.config.TargetedSweepRuntimeConfig;
import com.palantir.atlasdb.transaction.ImmutableTransactionConfig;
import com.palantir.atlasdb.transaction.TransactionConfig;
import java.util.Optional;
import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableAtlasDbRuntimeConfig.class)
@JsonSerialize(as = ImmutableAtlasDbRuntimeConfig.class)
@Value.Immutable
public abstract class AtlasDbRuntimeConfig {

    /**
     * Live reloadable configurations for background / legacy sweep.
     */
    @Value.Default
    public SweepConfig sweep() {
        return SweepConfig.defaultSweepConfig();
    }

    /**
     * Live reloadable configurations for targeted sweep. If the enableSweepQueueWrites parameter of
     * {@link AtlasDbConfig#targetedSweep()} is not set to true, this configuration will be ignored.
     */
    @Value.Default
    public TargetedSweepRuntimeConfig targetedSweep() {
        return TargetedSweepRuntimeConfig.defaultTargetedSweepRuntimeConfig();
    }

    @Value.Default
    public CompactorConfig compact() {
        return CompactorConfig.defaultCompactorConfig();
    }

    /**
     * Returns a configuration for this timestamp client.
     */
    @Value.Default
    public TimestampClientConfig timestampClient() {
        return ImmutableTimestampClientConfig.builder().build();
    }

    @Value.Default
    public TransactionConfig transaction() {
        return ImmutableTransactionConfig.builder().build();
    }

    @Value.Default
    public InternalSchemaConfig internalSchema() {
        return ImmutableInternalSchemaConfig.builder().build();
    }

    /**
     * The number of timestamps to cache that we have seen in previous reads.
     * This will use somewhere around 90MB of heap memory per million timestamps because of various overheads
     * from Java Objects and the cache's LRU tracking.
     *
     * Probably the only reason to configure away from the default would be a service that can afford the heap usage,
     * and has read patterns that deal with a very large working set of existing transactions.
     */
    @Value.Default
    public long getTimestampCacheSize() {
        return AtlasDbConstants.DEFAULT_TIMESTAMP_CACHE_SIZE;
    }

    public abstract Optional<KeyValueServiceRuntimeConfig> keyValueService();

    /**
     * Runtime live-reloadable parameters for communicating with TimeLock.
     *
     * We do not currently support live reloading from a leader block or using embedded services to using TimeLock.
     */
    public abstract Optional<TimeLockRuntimeConfig> timelockRuntime();

    @Value.Default
    public StreamStorePersistenceConfiguration streamStorePersistence() {
        return StreamStorePersistenceConfigurations.DEFAULT_CONFIG;
    }

    @Value.Default
    public RemotingClientConfig remotingClient() {
        return ImmutableRemotingClientConfig.builder().build();
    }

    /**
     * Feature flag for AtlasDB. If enabled (default), some AtlasDB metrics (e.g. targeted sweep) will only be produced
     * if they exceed certain thresholds.
     *
     * If debugging (e.g. P0) and you need metrics that are being filtered out, consider overriding this setting to
     * false.
     */
    @Value.Default
    public boolean enableMetricFiltering() {
        return true;
    }

    public static ImmutableAtlasDbRuntimeConfig defaultRuntimeConfig() {
        return ImmutableAtlasDbRuntimeConfig.builder().build();
    }

    public static ImmutableAtlasDbRuntimeConfig withSweepDisabled() {
        return ImmutableAtlasDbRuntimeConfig.builder()
                .sweep(SweepConfig.disabled())
                .targetedSweep(TargetedSweepRuntimeConfig.disabled())
                .build();
    }
}
