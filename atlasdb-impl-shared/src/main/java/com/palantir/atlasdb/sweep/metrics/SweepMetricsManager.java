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

import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;

@SuppressWarnings("checkstyle:FinalClass")
public class SweepMetricsManager {
    private final SweepMetricsFactory factory = new SweepMetricsFactory();

    private final SweepMetric cellsSweptMetric = factory.createDefault("cellTimestampPairsExamined");
    private final SweepMetric cellsDeletedMetric = factory.createDefault("staleValuesDeleted");
    private final SweepMetric sweepTimeSweepingMetric = factory.createDefault("sweepTimeSweeping");

    private final SweepMetric sweepTimeElapsedMetric = new SweepMetricsFactory.ListOfMetrics(
            factory.createCurrentValue("sweepTimeElapsedSinceStart", UpdateEvent.ONE_ITERATION, false),
            factory.createHistogram("sweepTimeElapsedSinceStart", UpdateEvent.FULL_TABLE, false));

    private final SweepMetric sweepErrorMetric = factory.createMeter("sweepError", UpdateEvent.ONE_ITERATION, false);

    private static final TableReference DUMMY = TableReference.createWithEmptyNamespace("dummy");

    public void updateMetrics(SweepResults sweepResults, TableReference tableRef, UpdateEvent updateEvent) {
        cellsSweptMetric.update(sweepResults.getCellTsPairsExamined(), tableRef, updateEvent);
        cellsDeletedMetric.update(sweepResults.getStaleValuesDeleted(), tableRef, updateEvent);
        sweepTimeSweepingMetric.update(sweepResults.getTimeInMillis(), tableRef, updateEvent);
        sweepTimeElapsedMetric.update(sweepResults.getTimeElapsedSinceStartedSweeping(), tableRef, updateEvent);
    }

    public void sweepError() {
        sweepErrorMetric.update(1, DUMMY, UpdateEvent.ONE_ITERATION);
    }
}
