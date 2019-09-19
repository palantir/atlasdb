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
package com.palantir.atlasdb.performance.benchmarks;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.performance.benchmarks.table.CleanModeratelyWideRowTable;
import com.palantir.atlasdb.performance.benchmarks.table.DirtyModeratelyWideRowTable;
import com.palantir.atlasdb.performance.benchmarks.table.ModeratelyWideRowTable;
import com.palantir.atlasdb.performance.benchmarks.table.Tables;
import com.palantir.atlasdb.performance.benchmarks.table.VeryWideRowTable;
import com.palantir.atlasdb.performance.benchmarks.table.WideRowTable;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@State(Scope.Benchmark)
public class TransactionGetRowsColumnRangeBenchmarks {

    @Benchmark
    @Threads(1)
    @Warmup(time = 16, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 160, timeUnit = TimeUnit.SECONDS)
    public void getAllColumnsSingleBigRow(VeryWideRowTable table, Blackhole blackhole) {
        getAllRowsAndAssert(table, blackhole,
                count -> Preconditions.checkState(count == table.getNumCols(),
                        "Should be %s columns, but was: %s", table.getNumCols(), count));
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 5, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 15, timeUnit = TimeUnit.SECONDS)
    public void getAllColumnsModeratelyWideRow(
            ModeratelyWideRowTable table,
            Blackhole blackhole) {
        getAllRowsAndAssert(table, blackhole,
                count -> Preconditions.checkState(count == table.getNumCols(),
                        "Should be %s columns, but was: %s", table.getNumCols(), count));
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 5, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 15, timeUnit = TimeUnit.SECONDS)
    public void getAllColumnsModeratelyWideRowWithSomeUncommitted(
            CleanModeratelyWideRowTable table,
            Blackhole blackhole) {
        getAllRowsAndAssert(table, blackhole,
                count -> Preconditions.checkState(count == table.getNumReadableCols(),
                        "Should be %s columns, but was: %s", table.getNumReadableCols(), count));
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 5, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 15, timeUnit = TimeUnit.SECONDS)
    public void getAllColumnsModeratelyWideRowWithManyUncommitted(
            DirtyModeratelyWideRowTable table,
            Blackhole blackhole) {
        getAllRowsAndAssert(table, blackhole,
                count -> Preconditions.checkState(count == table.getNumReadableCols(),
                        "Should be %s columns, but was: %s", table.getNumReadableCols(), count));
    }

    private void getAllRowsAndAssert(WideRowTable table, Blackhole blackhole, Consumer<Integer> assertion) {
        int rowsRead = table.getTransactionManager().runTaskThrowOnConflict(txn -> {
            Iterator<Map.Entry<Cell, byte[]>> iter = txn.getRowsColumnRange(
                    table.getTableRef(),
                    Collections.singleton(Tables.ROW_BYTES.array()),
                    new ColumnRangeSelection(null, null),
                    10000);
            int count = 0;
            while (iter.hasNext()) {
                blackhole.consume(iter.next());
                ++count;
            }
            return count;
        });
        assertion.accept(rowsRead);
    }
}
