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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

final class AutobatchMetrics {
    private final TaggedMetricRegistry taggedMetricRegistry;
    private final Map<String, AutobatchOverheadMetrics> cache = new ConcurrentHashMap<>();

    AutobatchMetrics(TaggedMetricRegistry taggedMetricRegistry) {
        this.taggedMetricRegistry = taggedMetricRegistry;
    }

    void markWaitAndRun(@CompileTimeConstant String operationType, Duration waitTime, Duration runningTime) {
        markWaitingTime(operationType, waitTime);
        markRunningTime(operationType, runningTime);

        Duration totalTime = waitTime.plus(runningTime);
        if (!totalTime.isZero()) {
            markWaitingTimeProportion(operationType, waitTime.dividedBy(totalTime));
        }
    }

    void markWaitingTime(@CompileTimeConstant String operationType, Duration waitTime) {
        getMetricsForOperationType(operationType)
                .waitTimeMillis()
                .update(waitTime.toMillis());

    }

    private void markRunningTime(@CompileTimeConstant String operationType, Duration runningTime) {
        getMetricsForOperationType(operationType)
                .runningTimeMillis()
                .update(runningTime.toMillis());

    }

    private void markWaitingTimeProportion(@CompileTimeConstant String operationType, long waitTimePercentage) {
        getMetricsForOperationType(operationType)
                .waitTimePercentage()
                .update(waitTimePercentage);

    }

    private AutobatchOverheadMetrics getMetricsForOperationType(String operationType) {
        return cache.computeIfAbsent(operationType, type -> {
            AutobatchOverheadMetrics metrics = AutobatchOverheadMetrics.builder()
                    .registry(taggedMetricRegistry)
                    .operationType(type)
                    .build();

            metrics.waitTimeMillisP1((Gauge<Double>) () -> metrics.waitTimeMillis().getSnapshot().getValue(0.01));
            metrics.waitTimeMillisP5((Gauge<Double>) () -> metrics.waitTimeMillis().getSnapshot().getValue(0.05));
            metrics.waitTimeMillisP50((Gauge<Double>) () -> metrics.waitTimeMillis().getSnapshot().getValue(0.5));
            metrics.waitTimeMillisP999((Gauge<Double>) () -> metrics.waitTimeMillis().getSnapshot().getValue(0.999));

            metrics.waitTimePercentageP1((Gauge<Double>) () -> metrics.waitTimePercentage().getSnapshot().getValue(0.01));
            metrics.waitTimePercentageP5((Gauge<Double>) () -> metrics.waitTimePercentage().getSnapshot().getValue(0.05));
            metrics.waitTimePercentageP50((Gauge<Double>) () -> metrics.waitTimePercentage().getSnapshot().getValue(0.5));
            metrics.waitTimeMillisP999((Gauge<Double>) () -> metrics.waitTimePercentage().getSnapshot().getValue(0.999));

            metrics.runningTimeMillisP1((Gauge<Double>) () -> metrics.runningTimeMillis().getSnapshot().getValue(0.01));
            metrics.runningTimeMillisP5((Gauge<Double>) () -> metrics.runningTimeMillis().getSnapshot().getValue(0.05));
            metrics.runningTimeMillisP50((Gauge<Double>) () -> metrics.runningTimeMillis().getSnapshot().getValue(0.5));
            metrics.runningTimeMillisP999((Gauge<Double>) () -> metrics.runningTimeMillis().getSnapshot().getValue(0.999));

            return metrics;
        });
    }

}
