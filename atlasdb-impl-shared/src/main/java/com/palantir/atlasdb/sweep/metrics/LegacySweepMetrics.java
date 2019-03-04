/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.sweep.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.util.CurrentValueMetric;

// Not final for Mockito
@SuppressWarnings("checkstyle:FinalClass")
public class LegacySweepMetrics {
    public static final String METRIC_BASE_NAME = LegacySweepMetrics.class.getName();

    private final Counter cellsExamined;
    private final Counter cellsDeleted;
    private final Counter timeSweeping;
    private final Counter sweepErrors;
    private final CurrentValueMetric<Long> totalTime;

    public LegacySweepMetrics(MetricRegistry metricRegistry) {
        this.cellsExamined = metricRegistry.counter(getMetricName(AtlasDbMetricNames.CELLS_EXAMINED));
        this.cellsDeleted = metricRegistry.counter(getMetricName(AtlasDbMetricNames.CELLS_SWEPT));
        this.timeSweeping = metricRegistry.counter(getMetricName(AtlasDbMetricNames.TIME_SPENT_SWEEPING));
        this.sweepErrors = metricRegistry.counter(getMetricName(AtlasDbMetricNames.SWEEP_ERROR));
        this.totalTime = new CurrentValueMetric<>();

        metricRegistry.gauge(getMetricName(AtlasDbMetricNames.TIME_ELAPSED_SWEEPING), () -> totalTime);
    }

    public void updateCellsExaminedDeleted(long cellTsPairsExamined, long staleValuesDeleted) {
        cellsExamined.inc(cellTsPairsExamined);
        cellsDeleted.inc(staleValuesDeleted);
    }

    public void updateSweepTime(long sweepTime, long totalTimeElapsedSweeping) {
        timeSweeping.inc(sweepTime);
        totalTime.setValue(totalTimeElapsedSweeping);
    }

    public void sweepError() {
        sweepErrors.inc();
    }

    private String getMetricName(String name) {
        return MetricRegistry.name(METRIC_BASE_NAME, name);
    }
}
