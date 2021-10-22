/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.client;

import com.palantir.atlasdb.timelock.api.ConjureTimelockService;
import com.palantir.lock.client.metrics.TimeLockFeedbackBackgroundTask;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.util.Optional;

public final class NamespacedConjureTimeLockServiceFactory {
    private NamespacedConjureTimeLockServiceFactory() {}

    public static NamespacedConjureTimelockService create(
            ConjureTimelockService conjureTimelockService,
            String timelockNamespace,
            Optional<TimeLockFeedbackBackgroundTask> timeLockFeedbackBackgroundTask,
            TaggedMetricRegistry taggedMetricRegistry) {
        LeaderElectionReportingTimelockService leaderElectionReportingTimelockService =
                LeaderElectionReportingTimelockService.create(conjureTimelockService, timelockNamespace);

        timeLockFeedbackBackgroundTask.ifPresent(
                task -> task.registerLeaderElectionStatistics(leaderElectionReportingTimelockService));

        return TimestampCorroboratingTimelockService.create(
                timelockNamespace, taggedMetricRegistry, leaderElectionReportingTimelockService);
    }
}
