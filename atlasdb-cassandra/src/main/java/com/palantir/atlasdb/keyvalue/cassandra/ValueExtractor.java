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
package com.palantir.atlasdb.keyvalue.cassandra;

import com.codahale.metrics.Counter;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.tracing.TraceStatistics;
import com.palantir.atlasdb.util.MetricsManager;
import java.util.Map;

class ValueExtractor extends ResultsExtractor<Value> {
    private final Map<Cell, Value> collector;
    private final Counter notLatestVisibleValueCellFilterCounter;

    ValueExtractor(
            MetricsManager metricsManager, Map<Cell, Value> collector, Counter notLatestVisibleValueCellFilterCounter) {
        super(metricsManager);
        this.collector = collector;
        this.notLatestVisibleValueCellFilterCounter = notLatestVisibleValueCellFilterCounter;
    }

    @Override
    public void internalExtractResult(
            long startTs, ColumnSelection selection, byte[] row, byte[] col, byte[] val, long ts) {
        if (ts < startTs && selection.contains(col)) {
            Cell cell = Cell.create(row, col);
            // explicitly not using `collector.computeIfAbsent` to avoid `LambdaForm.linkToTargetMethod` allocation
            if (collector.containsKey(cell)) {
                TraceStatistics.incSkippedValues(1L);
                notLatestVisibleValueCellFilterCounter.inc();
            } else {
                collector.put(cell, Value.create(val, ts));
            }
        } else {
            TraceStatistics.incSkippedValues(1L);
            notLatestVisibleValueCellFilterCounter.inc();
        }
    }

    @Override
    public Map<Cell, Value> asMap() {
        return collector;
    }
}
