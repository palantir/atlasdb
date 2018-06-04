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
    private final SweepMetric<Long> cellsExamined;
    private final SweepMetric<Long> cellsDeleted;
    private final SweepMetric<Long> timeSweeping;
    private final SweepMetric<Long> totalTime;
    private final SweepMetric<String> tableSweeping;
    private final SweepMetric<Long> sweepErrors;

    public SweepMetricsManager(SweepMetricsFactory factory) {
        cellsExamined = factory.accumulatingLong(AtlasDbMetricNames.CELLS_EXAMINED);
        cellsDeleted = factory.accumulatingLong(AtlasDbMetricNames.CELLS_SWEPT);
        timeSweeping = factory.simpleLong(AtlasDbMetricNames.TIME_SPENT_SWEEPING);
        totalTime = factory.simpleLong(AtlasDbMetricNames.TIME_ELAPSED_SWEEPING);
        tableSweeping = factory.simpleString(AtlasDbMetricNames.TABLE_BEING_SWEPT);
        sweepErrors = factory.simpleMeter(AtlasDbMetricNames.SWEEP_ERROR);
    }

    public void resetBeforeDeleteBatch() {
        cellsExamined.set(0L);
        cellsDeleted.set(0L);
    }

    public void updateAfterDeleteBatch(long cellTsPairsExamined, long staleValuesDeleted) {
        cellsExamined.accumulate(cellTsPairsExamined);
        cellsDeleted.accumulate(staleValuesDeleted);
    }

    public void updateMetrics(SweepResults sweepResults, TableReference tableRef) {
        cellsExamined.set(sweepResults.getCellTsPairsExamined());
        cellsDeleted.set(sweepResults.getStaleValuesDeleted());
        tableSweeping.set(LoggingArgs.safeTableOrPlaceholder(tableRef).getQualifiedName());
        timeSweeping.set(sweepResults.getTimeInMillis());
        totalTime.set(sweepResults.getTimeElapsedSinceStartedSweeping());
    }

    public void sweepError() {
        sweepErrors.set(1L);
    }
}
