/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.sweep.queue.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.AtlasDbConstants;
import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableTargetedSweepRuntimeConfig.class)
@JsonSerialize(as = ImmutableTargetedSweepRuntimeConfig.class)
@Value.Immutable
public abstract class TargetedSweepRuntimeConfig {
    /**
     * If true, targeted sweep will be performed in the background. Setting this to false will cause the background
     * threads to skip running sweeps, effectively pausing targeted sweep.
     */
    @Value.Default
    public boolean enabled() {
        return AtlasDbConstants.DEFAULT_ENABLE_TARGETED_SWEEP;
    }

    /**
     * The number of shards to be used for persisting targeted sweep information. Once the number of shards is increased
     * it cannot be reduced again, and setting the configuration to a lower value will have no effect. The maximum
     * allowed value is 256.
     */
    @Value.Default
    public int shards() {
        return 8;
    }

    /**
     * If true, we batch many iterations on each shard and strategy upon obtaining the lock. This should lead to
     * higher throughput in targeted sweep at the expense of more uneven sweeping across different shards.
     */
    @Value.Default
    public boolean batchShardIterations() {
        return false;
    }

    /**
     * Specifies the maximum number of (fine) partitions over which targeted sweep attempts to read sweep queue
     * information before executing deletes. Only partitions which actually contain information about writes will count
     * towards this limit. Targeted sweep may, of course, read fewer partitions. Legacy behaviour prior to the
     * introduction of this feature is consistent with a value of 1.
     *
     * This is expected to improve the throughput of targeted sweep, at the expense of more uneven sweeping across
     * different shards.
     */
    @Value.Default
    public int maximumPartitionsToBatchInSingleRead() {
        return 1;
    }

    @Value.Check
    void checkPartitionsToBatch() {
        Preconditions.checkArgument(maximumPartitionsToBatchInSingleRead() > 0,
                "Number of partitions to read in a batch must be positive, but found %s.",
                maximumPartitionsToBatchInSingleRead());
    }

    @Value.Check
    void checkShardSize() {
        Preconditions.checkArgument(shards() >= 1 && shards() <= 256,
                "Shard number must be between 1 and 256 inclusive, but it is %s.", shards());
    }

    @Value.Default
    public long pauseMillis() {
        return 500L;
    }

    public static TargetedSweepRuntimeConfig defaultTargetedSweepRuntimeConfig() {
        return ImmutableTargetedSweepRuntimeConfig.builder().build();
    }

    public static TargetedSweepRuntimeConfig disabled() {
        return ImmutableTargetedSweepRuntimeConfig.builder().enabled(false).build();
    }
}
