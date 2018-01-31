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

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;

// Not final for Mockito
@SuppressWarnings("checkstyle:FinalClass")
public class SweepMetricsManager {
    private static final TableReference DUMMY = TableReference.createWithEmptyNamespace("dummy");

    @VisibleForTesting
    final SweepMetricsFactory factory = new SweepMetricsFactory();

    private final SweepMetric cellsSweptMetric = factory.createDefault(AtlasDbMetricNames.CELLS_EXAMINED);
    private final SweepMetric cellsDeletedMetric = factory.createDefault(AtlasDbMetricNames.CELLS_SWEPT);
    private final SweepMetric sweepTimeSweepingMetric = factory.createDefault(AtlasDbMetricNames.TIME_SPENT_SWEEPING);
    private final SweepMetric tableBeingSwept =
            factory.createGaugeForTableBeingSwept(AtlasDbMetricNames.TABLE_BEING_SWEPT);
    private final SweepMetric sweepTimeElapsedMetric =
            factory.createMetricsForTimeElapsed(AtlasDbMetricNames.TIME_ELAPSED_SWEEPING);
    private final SweepMetric sweepErrorMetric =
            factory.createMeter(AtlasDbMetricNames.SWEEP_ERROR, UpdateEventType.ERROR, false);

    public void updateMetrics(SweepResults sweepResults, TableReference tableRef, UpdateEventType updateEvent) {
        cellsSweptMetric.update(sweepResults.getCellTsPairsExamined(), tableRef, updateEvent);
        cellsDeletedMetric.update(sweepResults.getStaleValuesDeleted(), tableRef, updateEvent);
        tableBeingSwept.update(LoggingArgs.safeTableOrPlaceholder(tableRef).getQualifiedName(), tableRef, updateEvent);
        sweepTimeSweepingMetric.update(sweepResults.getTimeInMillis(), tableRef, updateEvent);
        sweepTimeElapsedMetric.update(sweepResults.getTimeElapsedSinceStartedSweeping(), tableRef, updateEvent);
    }

    public void sweepError() {
        sweepErrorMetric.update(1L, DUMMY, UpdateEventType.ERROR);
    }
}
