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

import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;

// Not final for Mockito
@SuppressWarnings("checkstyle:FinalClass")
public class SweepMetricsManager {
    private static final TableReference DUMMY = TableReference.createWithEmptyNamespace("dummy");

    private final SweepMetricsFactory factory = new SweepMetricsFactory();

    private final SweepMetric<Long> cellsExamined = factory.accumulatingLong(AtlasDbMetricNames.CELLS_EXAMINED);
    private final SweepMetric<Long> cellsDeleted = factory.accumulatingLong(AtlasDbMetricNames.CELLS_SWEPT);
    private final SweepMetric<Long> timeSweeping = factory.simpleLong(AtlasDbMetricNames.TIME_SPENT_SWEEPING);
    private final SweepMetric<Long> totalTime = factory.simpleLong(AtlasDbMetricNames.TIME_ELAPSED_SWEEPING);
    private final SweepMetric<String> tableSweeping = factory.simpleString(AtlasDbMetricNames.TABLE_BEING_SWEPT);
    private final SweepMetric<Long> sweepErrors = factory.createMeter(AtlasDbMetricNames.SWEEP_ERROR, false);

    public void updateAfterDeleteBatch(long cellTsPairsExamined, long staleValuesDeleted) {
        cellsExamined.update(cellTsPairsExamined, DUMMY);
        cellsDeleted.update(staleValuesDeleted, DUMMY);
    }

    public void updateMetrics(SweepResults sweepResults, TableReference tableRef) {
        cellsExamined.set(sweepResults.getCellTsPairsExamined(), tableRef);
        cellsDeleted.set(sweepResults.getStaleValuesDeleted(), tableRef);
        tableSweeping.set(LoggingArgs.safeTableOrPlaceholder(tableRef).getQualifiedName(), tableRef);
        timeSweeping.set(sweepResults.getTimeInMillis(), tableRef);
        totalTime.set(sweepResults.getTimeElapsedSinceStartedSweeping(), tableRef);
    }

    public void sweepError() {
        sweepErrors.set(1L, DUMMY);
    }
}
