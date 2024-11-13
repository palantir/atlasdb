/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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
import java.time.Duration;
import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableAutoScalingTargetedSweepRuntimeConfig.class)
@JsonSerialize(as = ImmutableAutoScalingTargetedSweepRuntimeConfig.class)
@Value.Immutable
public interface AutoScalingTargetedSweepRuntimeConfig {
    // None of the durations are intended to be overridden. This exists solely as an escape hatch while we roll this out
    // Eventually, it's intended to be handled by a system that takes in the various autoscaling inputs and updates the
    // refreshables, and the config will be cleaned up to work with that system.
    // TODO(mdaudali): Address the above with the ASTS work.
    @Value.Default
    default Duration minWaitBetweenWorkRefresh() {
        return Duration.ofMinutes(2);
    }

    @Value.Default
    default Duration minAutoRefreshWorkInterval() {
        return Duration.ofMinutes(5);
    }

    @Value.Default
    default Duration minSingleSweepIterationInterval() {
        return Duration.ofSeconds(30);
    }

    @Value.Default
    default Duration minAssigningBucketsInterval() {
        return Duration.ofMinutes(5);
    }

    @Value.Default
    default Duration minShardProgressUpdaterInterval() {
        return Duration.ofMinutes(1);
    }

    // Modify the below config if you expect your service to churn through far more than 100_000_000 timestamps in a 10
    // minute window.
    @Value.Default
    default long maxCoarsePartitionsPerBucketForNonPuncherClose() {
        return 10; // 100_000_000L timestamps.
    }

    static AutoScalingTargetedSweepRuntimeConfig defaultConfig() {
        return ImmutableAutoScalingTargetedSweepRuntimeConfig.builder().build();
    }
}
