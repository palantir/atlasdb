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

package com.palantir.timelock.paxos;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.tritium.metrics.registry.MetricName;

class RejectionTrackingCallerRunsPolicy implements RejectedExecutionHandler {
    private final Counter rejectionCount;

    private RejectionTrackingCallerRunsPolicy(Counter rejectionCount) {
        this.rejectionCount = rejectionCount;
    }

    static RejectionTrackingCallerRunsPolicy createWithSafeLoggableUseCase(
            MetricsManager metricsManager,
            String safeLoggableUseCase) {
        return new RejectionTrackingCallerRunsPolicy(
                metricsManager.getTaggedRegistry().counter(MetricName.builder()
                        .safeName(MetricRegistry.name(RejectionTrackingCallerRunsPolicy.class, "rejection"))
                        .putSafeTags("useCase", safeLoggableUseCase)
                        .build()));
    }

    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        rejectionCount.inc();
        if (!executor.isShutdown()) {
            r.run();
        }
    }
}
