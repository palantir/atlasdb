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
package com.palantir.atlasdb.performance.benchmarks;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.performance.benchmarks.table.HeavilyScatteredUncommittedValueWideRowTable;
import com.palantir.atlasdb.performance.benchmarks.table.Tables;

@State(Scope.Benchmark)
public class TransactionGetRowsColumnRangeBenchmarks {

//    @Benchmark
//    @Threads(1)
//    @Warmup(time = 16, timeUnit = TimeUnit.SECONDS)
//    @Measurement(time = 80, timeUnit = TimeUnit.SECONDS)
//    public Object getAllColumnsSingleBigRow(VeryWideRowTable table, Blackhole blackhole) {
//        return table.getTransactionManager().runTaskThrowOnConflict(txn -> {
//            Iterator<Map.Entry<Cell, byte[]>> iter = txn.getRowsColumnRange(
//                    table.getTableRef(),
//                    Collections.singleton(Tables.ROW_BYTES.array()),
//                    new ColumnRangeSelection(null, null),
//                    10000);
//            int count = 0;
//            while (iter.hasNext()) {
//                blackhole.consume(iter.next());
//                ++count;
//            }
//            Preconditions.checkState(count == table.getNumCols(),
//                    "Should be %s columns, but were: %s", table.getNumCols(), count);
//            return count;
//        });
//    }

//    @Benchmark
//    @Threads(1)
//    @Warmup(time = 16, timeUnit = TimeUnit.SECONDS)
//    @Measurement(time = 80, timeUnit = TimeUnit.SECONDS)
//    public Object getAllColumnsSingleBigRowWithUncommitted_LightlyScattered(
//            LightlyScatteredUncommittedValueWideRowTable table,
//            Blackhole blackhole) {
//        return table.getTransactionManager().runTaskWithRetry(tx -> {
//            Iterator<Map.Entry<Cell, byte[]>> iter = tx.getRowsColumnRange(
//                    table.getTableRef(),
//                    Collections.singleton(Tables.ROW_BYTES.array()),
//                    new ColumnRangeSelection(null, null),
//                    10000);
//
//            int count = 0;
//            while (iter.hasNext()) {
//                blackhole.consume(iter.next());
//                ++count;
//            }
//            Preconditions.checkState(count == table.getTotalNumColumns(),
//                    "Should be %s columns, but was: %s", table.getTotalNumColumns());
//            return count;
//        });
//    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 1, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 7, timeUnit = TimeUnit.SECONDS)
    public Object getAllColumnsSingleBigRowWithUncommitted_HeavilyScattered(
            HeavilyScatteredUncommittedValueWideRowTable table,
            Blackhole blackhole) {
        return table.getTransactionManager().runTaskWithRetry(tx -> {
            Iterator<Map.Entry<Cell, byte[]>> iter = tx.getRowsColumnRange(
                    table.getTableRef(),
                    Collections.singleton(Tables.ROW_BYTES.array()),
                    new ColumnRangeSelection(null, null),
                    10000);

            int count = 0;

            List<Cell> cells = Lists.newArrayList();
            while (iter.hasNext()) {
                Cell cell = iter.next().getKey();
                cells.add(cell);
                ++count;
            }
            Preconditions.checkState(count == table.getTotalNumColumns(),
                    "Should be %s columns, but was: %s", table.getTotalNumColumns());
            for (int i = 1; i < count; i++) {
                Preconditions.checkState(cells.get(i - 1).compareTo(cells.get(i)) < 0,
                        "Must be sorted but wasn't! %s %s", cells.get(i-1), cells.get(i));
            }

            return count;
        });
    }
//
//    @Benchmark
//    @Threads(1)
//    @Warmup(time = 16, timeUnit = TimeUnit.SECONDS)
//    @Measurement(time = 80, timeUnit = TimeUnit.SECONDS)
//    public Object getAllColumnsSemiBigRow(ModeratelyWideRowTable table, Blackhole blackhole) {
//        return table.getTransactionManager().runTaskThrowOnConflict(txn -> {
//            Iterator<Map.Entry<Cell, byte[]>> iter = txn.getRowsColumnRange(
//                    table.getTableRef(),
//                    Collections.singleton(Tables.ROW_BYTES.array()),
//                    new ColumnRangeSelection(null, null),
//                    10000);
//            int count = 0;
//            while (iter.hasNext()) {
//                blackhole.consume(iter.next());
//                ++count;
//            }
//            Preconditions.checkState(count == table.getNumCols(),
//                    "Should be %s columns, but were: %s", table.getNumCols(), count);
//            return count;
//        });
//    }
}
