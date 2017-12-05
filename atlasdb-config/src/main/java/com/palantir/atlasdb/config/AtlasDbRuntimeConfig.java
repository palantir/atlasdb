/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.config;

import java.util.Optional;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.qos.config.QosClientConfig;

@JsonDeserialize(as = ImmutableAtlasDbRuntimeConfig.class)
@JsonSerialize(as = ImmutableAtlasDbRuntimeConfig.class)
@Value.Immutable
public abstract class AtlasDbRuntimeConfig {

    @Value.Default
    public SweepConfig sweep() {
        return SweepConfig.defaultSweepConfig();
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

    @Value.Default
    public QosClientConfig qos() {
        return QosClientConfig.DEFAULT;
    }

    /**
     * Runtime live-reloadable parameters for communicating with TimeLock.
     *
     * This value is ignored if the install config does not specify usage of TimeLock.
     * We do not currently support live reloading from a leader block or using embedded services to using TimeLock.
     */
    public abstract Optional<TimeLockRuntimeConfig> timelockRuntime();

    public static ImmutableAtlasDbRuntimeConfig defaultRuntimeConfig() {
        return ImmutableAtlasDbRuntimeConfig.builder().build();
    }

    public static ImmutableAtlasDbRuntimeConfig withoutSweep() {
        return ImmutableAtlasDbRuntimeConfig.builder().sweep(SweepConfig.disabled()).build();
    }
}
