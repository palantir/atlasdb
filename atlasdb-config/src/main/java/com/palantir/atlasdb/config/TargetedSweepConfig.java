/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.AtlasDbConstants;

@JsonDeserialize(as = ImmutableTargetedSweepConfig.class)
@JsonSerialize(as = ImmutableTargetedSweepConfig.class)
@Value.Immutable
public abstract class TargetedSweepConfig {
    /**
     * If targeted sweep is enabled, this parameter controls whether we run targeted sweeps or only persist the
     * necessary information to the targeted sweep queue. If runSweep is false, the necessary information is persisted
     * but no targeted sweeps will be run.
     */
    @Value.Default
    public boolean runSweep() {
        return AtlasDbConstants.DEFAULT_TARGETED_SWEEP_RUN;
    }

    /**
     * The number of shards to be used for persisting targeted sweep information. Once the number of shards is increased
     * it cannot be reduced again, and setting the configuration to a lower value will have no effect. The maximum
     * allowed value is 256.
     */
    @Value.Default
    public int shards() {
        return AtlasDbConstants.DEFAULT_TARGETED_SWEEP_SHARDS;
    }

    @Value.Check
    void checkShardSize() {
        Preconditions.checkArgument(shards() >= 1 && shards() <= 256,
                "Shard number must be between 1 and 256 inclusive, but it is %s.", shards());
    }

    public static TargetedSweepConfig defaultTargetedSweepConfig() {
        return ImmutableTargetedSweepConfig.builder().build();
    }
}
