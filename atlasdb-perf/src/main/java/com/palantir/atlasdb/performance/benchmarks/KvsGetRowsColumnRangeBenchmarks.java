/**
 * Copyright 2016 Palantir Technologies
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.performance.benchmarks.table.WideRowsTable;

@State(Scope.Benchmark)
public class KvsGetRowsColumnRangeBenchmarks {
    // This is an exact multiple of WideRowsTable.NUM_COLS_PER_ROW, so we never load partial rows
    private static final int ALIGNED_BATCH_HINT = 10000;
    // This is not an exact multiple of WideRowsTable.NUM_COLS_PER_ROW, so that we load partial rows for some batches
    private static final int UNALIGNED_BATCH_HINT = 10017;

    @Benchmark
    @Warmup(time = 5, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 45, timeUnit = TimeUnit.SECONDS)
    public Object getAllColumnsAligned(WideRowsTable table) {
        return getAllColumnsWithBatchHint(table, ALIGNED_BATCH_HINT);
    }

    @Benchmark
    @Warmup(time = 5, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 45, timeUnit = TimeUnit.SECONDS)
    public Object getAllColumnsUnaligned(WideRowsTable table) {
        return getAllColumnsWithBatchHint(table, UNALIGNED_BATCH_HINT);
    }

    private static List<Map.Entry<Cell, Value>> getAllColumnsWithBatchHint(WideRowsTable table, int cellBatchHint) {
        List<byte[]> rows =
                IntStream.rangeClosed(0, WideRowsTable.NUM_ROWS - 1)
                        .mapToObj(WideRowsTable::getRow)
                        .collect(Collectors.toList());
        RowColumnRangeIterator rowsColumnRange =
                table.getKvs().getRowsColumnRange(
                        table.getTableRef(),
                        rows,
                        new ColumnRangeSelection(null, null),
                        cellBatchHint,
                        Long.MAX_VALUE);
        int expectedNumCells = WideRowsTable.NUM_ROWS * WideRowsTable.NUM_COLS_PER_ROW;
        List<Map.Entry<Cell, Value>> loadedCells = new ArrayList<>(expectedNumCells);
        while (rowsColumnRange.hasNext()) {
            loadedCells.add(rowsColumnRange.next());
        }
        Preconditions.checkState(loadedCells.size() == expectedNumCells,
                "Should be %s cells, but were: %s", expectedNumCells, loadedCells.size());
        return loadedCells;
    }
}
