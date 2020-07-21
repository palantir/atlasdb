/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.leader.health;

import com.palantir.leader.LeaderElectionServiceMetrics;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public class LeaderElectionHealthCheck {
    private static final double MAX_ALLOWED_5_MIN_RATE = 5;
    private static final double MAX_ALLOWED_15_MIN_RATE = 15;

    private final LeaderElectionServiceMetrics leaderElectionServiceMetrics;

    public LeaderElectionHealthCheck(TaggedMetricRegistry taggedMetricRegistry) {
        this.leaderElectionServiceMetrics = LeaderElectionServiceMetrics.of(taggedMetricRegistry);
    }

    public LeaderElectionHealthStatus leaderElectionRateHealthStatus() {
        return leaderElectionServiceMetrics.proposedLeadership().getFiveMinuteRate() < MAX_ALLOWED_5_MIN_RATE
                ? LeaderElectionHealthStatus.HEALTHY : LeaderElectionHealthStatus.UNHEALTHY;
    }
}
