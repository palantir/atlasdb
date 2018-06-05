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

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.util.MetricsManager;

// Not final for Mockito
@SuppressWarnings("checkstyle:FinalClass")
public class SweepMetricsManager {
    private final MetricRegistry metricRegistry = new MetricsManager().getRegistry();

    private final Meter cellsExamined = metricRegistry.meter(getMetricName(AtlasDbMetricNames.CELLS_EXAMINED));
    private final Meter cellsDeleted = metricRegistry.meter(getMetricName(AtlasDbMetricNames.CELLS_SWEPT));
    private final Meter timeSweeping = metricRegistry.meter(getMetricName(AtlasDbMetricNames.TIME_SPENT_SWEEPING));
    private final Meter totalTime = metricRegistry.meter(getMetricName(AtlasDbMetricNames.TIME_ELAPSED_SWEEPING));
    private final Meter sweepErrors = metricRegistry.meter(getMetricName(AtlasDbMetricNames.SWEEP_ERROR));


    public void updateAfterDeleteBatch(long cellTsPairsExamined, long staleValuesDeleted) {
        cellsExamined.mark(cellTsPairsExamined);
        cellsDeleted.mark(staleValuesDeleted);
    }

    public void updateMetrics(SweepResults sweepResults) {
        cellsExamined.mark(sweepResults.getCellTsPairsExamined());
        cellsDeleted.mark(sweepResults.getStaleValuesDeleted());
        timeSweeping.mark(sweepResults.getTimeInMillis());
        totalTime.mark(sweepResults.getTimeElapsedSinceStartedSweeping());
    }

    public void sweepError() {
        sweepErrors.mark(1L);
    }

    private String getMetricName(String name) {
        return MetricRegistry.name(SweepMetric.class, name);
    }
}
