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

    public static final int EXPECTED_NUM_CELLS = WideRowsTable.NUM_ROWS * WideRowsTable.NUM_COLS_PER_ROW;
    public static final List<byte[]> ROWS = IntStream
            .rangeClosed(0, WideRowsTable.NUM_ROWS - 1)
            .mapToObj(WideRowsTable::getRow)
            .collect(Collectors.toList());
    public static final int CORRECT_CELL_BATCH_HINT = 10000;
    public static final int INCORRECT_CELL_BATCH_HINT = 10017;

    @Benchmark
    @Warmup(time = 16, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 160, timeUnit = TimeUnit.SECONDS)
    public Object getAllColumnsAligned(WideRowsTable table) {
        List<Map.Entry<Cell, Value>> loadedCells = getEntriesFromGetRowsColumnRange(table, CORRECT_CELL_BATCH_HINT);
        assertCorrectness(loadedCells);
        return loadedCells;
    }

    @Benchmark
    @Warmup(time = 16, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 160, timeUnit = TimeUnit.SECONDS)
    public Object getAllColumnsUnaligned(WideRowsTable table) {
        List<Map.Entry<Cell, Value>> loadedCells = getEntriesFromGetRowsColumnRange(table, INCORRECT_CELL_BATCH_HINT);
        assertCorrectness(loadedCells);
        return loadedCells;
    }

    private List<Map.Entry<Cell, Value>> getEntriesFromGetRowsColumnRange(WideRowsTable table, int batchHint) {
        RowColumnRangeIterator rowsColumnRange =
                table.getKvs().getRowsColumnRange(
                        table.getTableRef(),
                        ROWS,
                        new ColumnRangeSelection(null, null),
                        batchHint,
                        Long.MAX_VALUE);
        List<Map.Entry<Cell, Value>> loadedCells = new ArrayList<>(EXPECTED_NUM_CELLS);
        while (rowsColumnRange.hasNext()) {
            loadedCells.add(rowsColumnRange.next());
        }
        return loadedCells;
    }

    private void assertCorrectness(List<Map.Entry<Cell, Value>> loadedCells) {
        Preconditions.checkState(loadedCells.size() == EXPECTED_NUM_CELLS,
                "Should be %s cells, but were: %s", EXPECTED_NUM_CELLS, loadedCells.size());
    }
}
