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
import com.palantir.atlasdb.sweep.metrics.TargetedSweepMetrics;
import com.palantir.atlasdb.sweep.metrics.TargetedSweepMetricsConfigurations;
import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableTargetedSweepInstallConfig.class)
@JsonSerialize(as = ImmutableTargetedSweepInstallConfig.class)
@Value.Immutable
public class TargetedSweepInstallConfig {
    /**
     * If true, information for targeted sweep will be persisted to the sweep queue. This is necessary to be able to
     * run targeted sweep.
     *
     * Once you decide to use targeted sweep, DO NOT set this to false without consulting with the AtlasDB team. Doing
     * so will cause targeted sweep to not be aware of any writes occurring while this is false, and you will have to
     * use legacy sweep to sweep those writes. If you wish to pause targeted sweep, that can be done by setting the live
     * reloadable {@link TargetedSweepRuntimeConfig#enabled()} parameter to false.
     */
    @Value.Default
    public boolean enableSweepQueueWrites() {
        return AtlasDbConstants.DEFAULT_ENABLE_SWEEP_QUEUE_WRITES;
    }

    /**
     * The number of background threads dedicated to running targeted sweep of tables with SweepStrategy CONSERVATIVE.
     */
    @Value.Default
    public int conservativeThreads() {
        return AtlasDbConstants.DEFAULT_TARGETED_SWEEP_THREADS;
    }

    @Value.Check
    void checkConservativeThreads() {
        Preconditions.checkArgument(
                conservativeThreads() >= 0 && conservativeThreads() <= AtlasDbConstants.MAX_SWEEP_QUEUE_SHARDS,
                "Number of conservative targeted sweep threads must be between 0 and %s inclusive, but is %s instead.",
                AtlasDbConstants.MAX_SWEEP_QUEUE_SHARDS,
                conservativeThreads());
    }

    /**
     * The number of background threads dedicated to running targeted sweep of tables with SweepStrategy THOROUGH.
     */
    @Value.Default
    public int thoroughThreads() {
        return AtlasDbConstants.DEFAULT_TARGETED_SWEEP_THREADS;
    }

    @Value.Check
    void checkThoroughThreads() {
        Preconditions.checkArgument(
                thoroughThreads() >= 0 && thoroughThreads() <= AtlasDbConstants.MAX_SWEEP_QUEUE_SHARDS,
                "Number of thorough targeted sweep threads must be between 0 and %s inclusive, but is %s instead.",
                AtlasDbConstants.MAX_SWEEP_QUEUE_SHARDS,
                thoroughThreads());
    }

    /**
     * Specifies how metrics are tracked for this instance of Targeted Sweep.
     */
    @Value.Default
    public TargetedSweepMetrics.MetricsConfiguration metricsConfiguration() {
        return TargetedSweepMetricsConfigurations.DEFAULT;
    }

    /**
     * Specifies whether on startup we should reset progress in the targeted sweep queue. This may be useful to deal
     * with circumstances where entries are written to the targeted sweep queue after the sweep timestamp has
     * progressed past it - while the transaction in question will necessarily fail, there may still be cruft in the
     * targeted sweep queue.
     *
     * If set to true, resets progress to zero for each shard and strategy on startup. This configuration can also only
     * be safely used if nodes are not actively sweeping, and so if configured to be true will prevent targeted sweep
     * from running.
     */
    @Value.Default
    public boolean resetTargetedSweepQueueProgressAndStopSweep() {
        return false;
    }

    public static TargetedSweepInstallConfig defaultTargetedSweepConfig() {
        return ImmutableTargetedSweepInstallConfig.builder().build();
    }
}
