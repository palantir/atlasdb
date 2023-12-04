/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.autobatch;

import com.codahale.metrics.Gauge;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.time.Duration;

public final class AutobatcherTelemetryComponents {
    private final String safeLoggablePurpose;
    private final AutobatchOverheadMetrics overheadMetrics;

    private AutobatcherTelemetryComponents(String safeLoggablePurpose, AutobatchOverheadMetrics overheadMetrics) {
        this.safeLoggablePurpose = safeLoggablePurpose;
        this.overheadMetrics = overheadMetrics;
    }

    void markWaitingTimeAndRunningTimeMetrics(Duration waitTime, Duration runningTime) {
        markWaitingTimeMetrics(waitTime);
        markRunningTimeMetrics(runningTime);

        long totalTimeNanos = waitTime.toNanos() + runningTime.toNanos();
        if (totalTimeNanos > 0) {
            markWaitingTimePercentage((100 * waitTime.toNanos()) / totalTimeNanos);
        }
    }

    void markWaitingTimeMetrics(Duration waitTime) {
        overheadMetrics.waitTimeNanos().update(waitTime.toNanos());
    }

    String getSafeLoggablePurpose() {
        return safeLoggablePurpose;
    }

    private void markRunningTimeMetrics(Duration runningTime) {
        overheadMetrics.runningTimeNanos().update(runningTime.toNanos());
    }

    private void markWaitingTimePercentage(long waitTimePercentage) {
        overheadMetrics.waitTimePercentage().update(waitTimePercentage);
    }

    public static AutobatcherTelemetryComponents create(
            String safeLoggablePurpose, TaggedMetricRegistry taggedMetricRegistry) {
        AutobatchOverheadMetrics overheadMetrics = AutobatchOverheadMetrics.builder()
                .registry(taggedMetricRegistry)
                .operationType(safeLoggablePurpose)
                .build();

        overheadMetrics.waitTimeNanosP1((Gauge<Double>)
                () -> overheadMetrics.waitTimeNanos().getSnapshot().getValue(0.01));
        overheadMetrics.waitTimeNanosP5((Gauge<Double>)
                () -> overheadMetrics.waitTimeNanos().getSnapshot().getValue(0.05));
        overheadMetrics.waitTimeNanosMedian((Gauge<Double>)
                () -> overheadMetrics.waitTimeNanos().getSnapshot().getValue(0.5));
        overheadMetrics.waitTimeNanosP999((Gauge<Double>)
                () -> overheadMetrics.waitTimeNanos().getSnapshot().getValue(0.999));

        overheadMetrics.waitTimePercentageP1((Gauge<Double>)
                () -> overheadMetrics.waitTimePercentage().getSnapshot().getValue(0.01));
        overheadMetrics.waitTimePercentageP5((Gauge<Double>)
                () -> overheadMetrics.waitTimePercentage().getSnapshot().getValue(0.05));
        overheadMetrics.waitTimePercentageMedian((Gauge<Double>)
                () -> overheadMetrics.waitTimePercentage().getSnapshot().getValue(0.5));
        overheadMetrics.waitTimePercentageP999((Gauge<Double>)
                () -> overheadMetrics.waitTimePercentage().getSnapshot().getValue(0.999));

        overheadMetrics.runningTimeNanosP1((Gauge<Double>)
                () -> overheadMetrics.runningTimeNanos().getSnapshot().getValue(0.01));
        overheadMetrics.runningTimeNanosP5((Gauge<Double>)
                () -> overheadMetrics.runningTimeNanos().getSnapshot().getValue(0.05));
        overheadMetrics.runningTimeNanosMedian((Gauge<Double>)
                () -> overheadMetrics.runningTimeNanos().getSnapshot().getValue(0.5));
        overheadMetrics.runningTimeNanosP999((Gauge<Double>)
                () -> overheadMetrics.runningTimeNanos().getSnapshot().getValue(0.999));

        return new AutobatcherTelemetryComponents(safeLoggablePurpose, overheadMetrics);
    }
}
