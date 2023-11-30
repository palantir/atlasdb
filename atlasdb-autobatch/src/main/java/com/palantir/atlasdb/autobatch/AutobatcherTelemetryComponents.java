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
import com.google.errorprone.annotations.CompileTimeConstant;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.time.Duration;

public final class AutobatcherTelemetryComponents {
    private final @CompileTimeConstant String safeLoggablePurpose;
    private final AutobatchOverheadMetrics overheadMetrics;

    private AutobatcherTelemetryComponents(@CompileTimeConstant String safeLoggablePurpose,  AutobatchOverheadMetrics overheadMetrics) {
        this.safeLoggablePurpose = safeLoggablePurpose;
        this.overheadMetrics = overheadMetrics;
    }

    void markWaitingTimeAndRunningTimeMetrics(Duration waitTime, Duration runningTime) {
        markWaitingTimeMetrics(waitTime);
        markRunningTime(runningTime);

        Duration totalTime = waitTime.plus(runningTime);
        if (!totalTime.isZero()) {
            markWaitingTimeProportion(waitTime.dividedBy(totalTime));
        }
    }

    void markWaitingTimeMetrics(Duration waitTime) {
        overheadMetrics
                .waitTimeMillis()
                .update(waitTime.toMillis());

    }

    String getSafeLoggablePurpose() {
        return safeLoggablePurpose;
    }

    private void markRunningTime(Duration runningTime) {
        overheadMetrics
                .runningTimeMillis()
                .update(runningTime.toMillis());

    }

    private void markWaitingTimeProportion(long waitTimePercentage) {
        overheadMetrics
                .waitTimePercentage()
                .update(waitTimePercentage);

    }

    public static AutobatcherTelemetryComponents create(@CompileTimeConstant String safeLoggablePurpose, TaggedMetricRegistry taggedMetricRegistry) {
        AutobatchOverheadMetrics overheadMetrics = AutobatchOverheadMetrics.builder()
                .registry(taggedMetricRegistry)
                .operationType(safeLoggablePurpose)
                .build();

        overheadMetrics.waitTimeMillisP1((Gauge<Double>) () -> overheadMetrics.waitTimeMillis().getSnapshot().getValue(0.01));
        overheadMetrics.waitTimeMillisP5((Gauge<Double>) () -> overheadMetrics.waitTimeMillis().getSnapshot().getValue(0.05));
        overheadMetrics.waitTimeMillisP50((Gauge<Double>) () -> overheadMetrics.waitTimeMillis().getSnapshot().getValue(0.5));
        overheadMetrics.waitTimeMillisP999((Gauge<Double>) () -> overheadMetrics.waitTimeMillis().getSnapshot().getValue(0.999));

        overheadMetrics.waitTimePercentageP1((Gauge<Double>) () -> overheadMetrics.waitTimePercentage().getSnapshot().getValue(0.01));
        overheadMetrics.waitTimePercentageP5((Gauge<Double>) () -> overheadMetrics.waitTimePercentage().getSnapshot().getValue(0.05));
        overheadMetrics.waitTimePercentageP50((Gauge<Double>) () -> overheadMetrics.waitTimePercentage().getSnapshot().getValue(0.5));
        overheadMetrics.waitTimeMillisP999((Gauge<Double>) () -> overheadMetrics.waitTimePercentage().getSnapshot().getValue(0.999));

        overheadMetrics.runningTimeMillisP1((Gauge<Double>) () -> overheadMetrics.runningTimeMillis().getSnapshot().getValue(0.01));
        overheadMetrics.runningTimeMillisP5((Gauge<Double>) () -> overheadMetrics.runningTimeMillis().getSnapshot().getValue(0.05));
        overheadMetrics.runningTimeMillisP50((Gauge<Double>) () -> overheadMetrics.runningTimeMillis().getSnapshot().getValue(0.5));
        overheadMetrics.runningTimeMillisP999((Gauge<Double>) () -> overheadMetrics.runningTimeMillis().getSnapshot().getValue(0.999));

        return new AutobatcherTelemetryComponents(safeLoggablePurpose, overheadMetrics);
    }


}
