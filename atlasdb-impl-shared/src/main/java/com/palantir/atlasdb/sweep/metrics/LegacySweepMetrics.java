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
import com.google.common.base.Suppliers;
import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.util.CurrentValueMetric;
import java.util.function.Supplier;
import org.immutables.value.Value;

// Not final for Mockito
@SuppressWarnings("checkstyle:FinalClass")
@Value.Enclosing
public class LegacySweepMetrics {
    static final String METRIC_BASE_NAME = LegacySweepMetrics.class.getName();

    private final Supplier<Metrics> metrics;

    public LegacySweepMetrics(MetricRegistry metricRegistry) {
        metrics = Suppliers.memoize(() -> buildMetrics(metricRegistry));
    }

    public void updateCellsExaminedDeleted(long cellTsPairsExamined, long staleValuesDeleted) {
        metrics.get().cellsExamined().inc(cellTsPairsExamined);
        metrics.get().cellsDeleted().inc(staleValuesDeleted);
    }

    public void updateSweepTime(long sweepTime, long totalTimeElapsedSweeping) {
        metrics.get().timeSweeping().inc(sweepTime);
        metrics.get().totalTime().setValue(totalTimeElapsedSweeping);
    }

    public void sweepError() {
        metrics.get().sweepErrors().inc();
    }

    private Metrics buildMetrics(MetricRegistry metricRegistry) {
        CurrentValueMetric<Long> totalTime = new CurrentValueMetric<>();
        metricRegistry.gauge(getMetricName(AtlasDbMetricNames.TIME_ELAPSED_SWEEPING), () -> totalTime);
        return ImmutableLegacySweepMetrics.Metrics.builder()
                .cellsExamined(getCounter(metricRegistry, AtlasDbMetricNames.CELLS_EXAMINED))
                .cellsDeleted(getCounter(metricRegistry, AtlasDbMetricNames.CELLS_SWEPT))
                .timeSweeping(getCounter(metricRegistry, AtlasDbMetricNames.TIME_SPENT_SWEEPING))
                .sweepErrors(getCounter(metricRegistry, AtlasDbMetricNames.SWEEP_ERROR))
                .totalTime(totalTime)
                .build();
    }

    private Counter getCounter(MetricRegistry metricRegistry, String name) {
        return metricRegistry.counter(getMetricName(name));
    }

    private String getMetricName(String name) {
        return MetricRegistry.name(METRIC_BASE_NAME, name);
    }

    @Value.Immutable
    interface Metrics {
        Counter cellsExamined();

        Counter cellsDeleted();

        Counter timeSweeping();

        Counter sweepErrors();

        CurrentValueMetric<Long> totalTime();
    }
}
