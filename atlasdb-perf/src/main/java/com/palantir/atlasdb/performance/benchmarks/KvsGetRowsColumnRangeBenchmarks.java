/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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
 *
 */

package com.palantir.atlasdb.performance.benchmarks;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.performance.benchmarks.table.Tables;
import com.palantir.atlasdb.performance.benchmarks.table.VeryWideRowTable;
import com.palantir.atlasdb.performance.benchmarks.table.WideRowsTable;
import com.palantir.common.base.BatchingVisitables;

@State(Scope.Benchmark)
public class KvsGetRowsColumnRangeBenchmarks {

    @Benchmark
    @Threads(1)
    @Warmup(time = 16, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 160, timeUnit = TimeUnit.SECONDS)
    public Object getAllColumnsAligned(WideRowsTable table) {
        List<byte[]> rows =
                IntStream.rangeClosed(0, WideRowsTable.NUM_ROWS - 1)
                        .mapToObj(WideRowsTable::getRow)
                        .collect(Collectors.toList());
        List<Map.Entry<Cell, Value>> loadedCells = ImmutableList.copyOf(Iterators.concat(
                table.getKvs().getRowsColumnRange(
                        table.getTableRef(),
                        rows,
                        BatchColumnRangeSelection.create(null, null, 10000),
                        Long.MAX_VALUE).values().iterator()));
        int expectedNumCells = WideRowsTable.NUM_ROWS * WideRowsTable.NUM_COLS_PER_ROW;
        Preconditions.checkState(loadedCells.size() == expectedNumCells,
                "Should be %s cells, but were: %s", expectedNumCells, loadedCells.size());
        return loadedCells;
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 16, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 160, timeUnit = TimeUnit.SECONDS)
    public Object getAllColumnsUnaligned(WideRowsTable table) {
        List<byte[]> rows =
                IntStream.rangeClosed(0, WideRowsTable.NUM_ROWS - 1)
                        .mapToObj(WideRowsTable::getRow)
                        .collect(Collectors.toList());
        List<Map.Entry<Cell, Value>> loadedCells = ImmutableList.copyOf(Iterators.concat(
                table.getKvs().getRowsColumnRange(
                        table.getTableRef(),
                        rows,
                        BatchColumnRangeSelection.create(null, null, 10017),
                        Long.MAX_VALUE).values().iterator()));
        int expectedNumCells = WideRowsTable.NUM_ROWS * WideRowsTable.NUM_COLS_PER_ROW;
        Preconditions.checkState(loadedCells.size() == expectedNumCells,
                "Should be %s cells, but were: %s", expectedNumCells, loadedCells.size());
        return loadedCells;
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 16, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 160, timeUnit = TimeUnit.SECONDS)
    public Object getAllColumnsSingleBigRow(VeryWideRowTable table,
                                            Blackhole blackhole) {
        Iterator<Map.Entry<Cell, Value>> iter = Iterators.concat(
                table.getKvs().getRowsColumnRange(
                        table.getTableRef(),
                        Collections.singleton(Tables.ROW_BYTES.array()),
                        BatchColumnRangeSelection.create(null, null, 10000),
                        Long.MAX_VALUE).values().iterator());
        int count = 0;
        while (iter.hasNext()) {
            blackhole.consume(iter.next());
            ++count;
        }
        Preconditions.checkState(count == table.getNumCols(),
                "Should be %s cells, but were: %s", table.getNumCols(), count);
        return count;
    }

}
