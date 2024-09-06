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
import com.codahale.metrics.Histogram;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.time.Duration;

public final class AutobatcherTelemetryComponents {
    private final String safeLoggablePurpose;
    private final AutobatchOverheadMetrics overheadMetrics;
    private final Histogram waitTimer;
    private final Histogram runningTimer;
    private final Histogram waitTimeHistogram;
    private final Histogram totalTimer;

    private AutobatcherTelemetryComponents(String safeLoggablePurpose, AutobatchOverheadMetrics overheadMetrics) {
        this.safeLoggablePurpose = safeLoggablePurpose;
        this.overheadMetrics = overheadMetrics;
        this.waitTimer = overheadMetrics.waitTimeNanos();
        this.runningTimer = overheadMetrics.runningTimeNanos();
        this.waitTimeHistogram = overheadMetrics.waitTimePercentage();
        this.totalTimer = overheadMetrics.totalTimeNanos();
    }

    String getSafeLoggablePurpose() {
        return safeLoggablePurpose;
    }

    void markWaitingTimeAndRunningTimeMetrics(Duration waitTime, Duration runningTime) {
        markWaitingTimeMetrics(waitTime);
        markRunningTimeMetrics(runningTime);

        long totalTimeNanos = waitTime.toNanos() + runningTime.toNanos();
        markTotalTimeMetrics(totalTimeNanos);
        if (totalTimeNanos > 0) {
            markWaitingTimePercentage((100 * waitTime.toNanos()) / totalTimeNanos);
        }
    }

    void markWaitingTimeMetrics(Duration waitTime) {
        this.waitTimer.update(waitTime.toNanos());
    }

    private void markRunningTimeMetrics(Duration runningTime) {
        this.runningTimer.update(runningTime.toNanos());
    }

    private void markTotalTimeMetrics(long totalTimeNanos) {
        this.totalTimer.update(totalTimeNanos);
    }

    private void markWaitingTimePercentage(long waitTimePercentage) {
        this.waitTimeHistogram.update(waitTimePercentage);
    }

    public static AutobatcherTelemetryComponents create(
            String safeLoggablePurpose, TaggedMetricRegistry taggedMetricRegistry) {
        AutobatchOverheadMetrics overheadMetrics = AutobatchOverheadMetrics.builder()
                .registry(taggedMetricRegistry)
                .operationType(safeLoggablePurpose)
                .build();

        AutobatcherTelemetryComponents autobatcherTelemetry =
                new AutobatcherTelemetryComponents(safeLoggablePurpose, overheadMetrics);
        autobatcherTelemetry.registerGauges();
        return autobatcherTelemetry;
    }

    private void registerGauges() {
        overheadMetrics.waitTimeNanosP1(
                (Gauge<Double>) () -> waitTimer.getSnapshot().getValue(0.01));
        overheadMetrics.waitTimeNanosMedian(
                (Gauge<Double>) () -> waitTimer.getSnapshot().getValue(0.5));

        overheadMetrics.waitTimePercentageP1(
                (Gauge<Double>) () -> waitTimeHistogram.getSnapshot().getValue(0.01));
        overheadMetrics.waitTimePercentageMedian(
                (Gauge<Double>) () -> waitTimeHistogram.getSnapshot().getValue(0.5));

        overheadMetrics.runningTimeNanosP1(
                (Gauge<Double>) () -> runningTimer.getSnapshot().getValue(0.01));
        overheadMetrics.runningTimeNanosMedian(
                (Gauge<Double>) () -> runningTimer.getSnapshot().getValue(0.5));

        overheadMetrics.totalTimeNanosP1(
                (Gauge<Double>) () -> totalTimer.getSnapshot().getValue(0.01));
        overheadMetrics.totalTimeNanosMedian(
                (Gauge<Double>) () -> totalTimer.getSnapshot().getValue(0.5));
    }
}
