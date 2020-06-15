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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.palantir.common.concurrent.PTExecutors;
import com.palantir.lock.client.ConjureTimelockServiceBlockingMetrics;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public class TimeLockFeedbackBackgroundTask implements AutoCloseable {
    private final ScheduledExecutorService executor = PTExecutors.newSingleThreadScheduledExecutor();
    private final UUID nodeId = UUID.randomUUID();
    private final int timeLockClientFeedbackReportInterval = 30;
    private ConjureTimelockServiceBlockingMetrics conjureTimelockServiceBlockingMetrics;
    private Supplier<String> versionSupplier;
    private String serviceName;

    private TimeLockFeedbackBackgroundTask(TaggedMetricRegistry taggedMetricRegistry, Supplier<String> versionSupplier,
            String serviceName) {
        this.conjureTimelockServiceBlockingMetrics = ConjureTimelockServiceBlockingMetrics.of(taggedMetricRegistry);
        this.versionSupplier = versionSupplier;
        this.serviceName = serviceName;
    }

    public static TimeLockFeedbackBackgroundTask create(TaggedMetricRegistry taggedMetricRegistry, Supplier<String> versionSupplier,
            String serviceName) {
        TimeLockFeedbackBackgroundTask task = new TimeLockFeedbackBackgroundTask(taggedMetricRegistry,
                versionSupplier, serviceName);
        task.scheduleWithFixedDelay();
        return task;
    }


    public void scheduleWithFixedDelay() {
        executor.scheduleWithFixedDelay(() -> {
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
        }, timeLockClientFeedbackReportInterval, timeLockClientFeedbackReportInterval, TimeUnit.SECONDS);
    }

    @Override
    public void close() {
        executor.shutdown();
    }
}
