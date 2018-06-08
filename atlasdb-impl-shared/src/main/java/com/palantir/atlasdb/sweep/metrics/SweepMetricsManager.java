/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.sweep.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.util.MetricsManager;

// Not final for Mockito
@SuppressWarnings("checkstyle:FinalClass")
public class SweepMetricsManager {
    public static final String METRIC_BASE_NAME = SweepMetricsManager.class.getPackage().getName() + ".SweepMetric";

    private final MetricRegistry metricRegistry = new MetricsManager().getRegistry();

    private final Counter cellsExamined = metricRegistry.counter(getMetricName(AtlasDbMetricNames.CELLS_EXAMINED));
    private final Counter cellsDeleted = metricRegistry.counter(getMetricName(AtlasDbMetricNames.CELLS_SWEPT));
    private final Counter timeSweeping = metricRegistry.counter(getMetricName(AtlasDbMetricNames.TIME_SPENT_SWEEPING));
    private final Counter totalTime = metricRegistry.counter(getMetricName(AtlasDbMetricNames.TIME_ELAPSED_SWEEPING));
    private final Counter sweepErrors = metricRegistry.counter(getMetricName(AtlasDbMetricNames.SWEEP_ERROR));


    public void updateCellExaminedDeleted(long cellTsPairsExamined, long staleValuesDeleted) {
        cellsExamined.inc(cellTsPairsExamined);
        cellsDeleted.inc(staleValuesDeleted);
    }

    public void updateSweepTime(long sweepTime, long totalTimeElapsedSweeping) {
        timeSweeping.inc(sweepTime);
        totalTime.inc(totalTimeElapsedSweeping);
    }

    public void sweepError() {
        sweepErrors.inc();
    }

    private String getMetricName(String name) {
        return MetricRegistry.name(METRIC_BASE_NAME, name);
    }
}
