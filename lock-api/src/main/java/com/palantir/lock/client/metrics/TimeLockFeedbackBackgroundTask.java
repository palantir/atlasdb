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

package com.palantir.lock.client.metrics;


import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import com.palantir.lock.client.ConjureTimelockServiceBlockingMetrics;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public class TimeLockFeedbackBackgroundTask {
    private TimeLockFeedbackBackgroundTask() {
        // no op
    }

    public static void create(TaggedMetricRegistry taggedMetricRegistry, Supplier<String> versionSupplier,
            String serviceName) {
        UUID nodeId = UUID.randomUUID();

        ScheduledExecutorService scheduledExecutorService =
                Executors.newScheduledThreadPool(5);
        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            ConjureTimelockServiceBlockingMetrics conjureTimelockServiceBlockingMetrics =
                    ConjureTimelockServiceBlockingMetrics.of(taggedMetricRegistry); //todo sudiksha

            ImmutableClientFeedback
                    .builder()
                    .percentile99th(conjureTimelockServiceBlockingMetrics
                            .startTransactions()
                            .getSnapshot()
                            .get99thPercentile())
                    .oneMinuteRate(conjureTimelockServiceBlockingMetrics.startTransactions().getOneMinuteRate())
                    .atlasVersion(versionSupplier.get())
                    .nodeId(nodeId)
                    .serviceName(serviceName)
                    .build();
        }, 30, 30, TimeUnit.SECONDS);

    }
}
