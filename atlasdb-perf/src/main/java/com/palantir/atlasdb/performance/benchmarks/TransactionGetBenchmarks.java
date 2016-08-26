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
 */

package com.palantir.atlasdb.performance.benchmarks;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import com.google.common.collect.Iterables;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.performance.benchmarks.table.ConsecutiveNarrowTable;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.common.base.BatchingVisitables;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 1, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 1, time = 30, timeUnit = TimeUnit.SECONDS)
public class TransactionGetBenchmarks {

    private static final int RANGES_SINGLE_REQUEST_SIZE = 1;

    private Map<Cell, byte[]> getCellsInner(ConsecutiveNarrowTable table, int numCells) {
        return table.getTransactionManager().runTaskThrowOnConflict(txn -> {
            Set<Cell> request = table.getCellsRequest(numCells);
            Map<Cell, byte[]> result = txn.get(ConsecutiveNarrowTable.TABLE_REF, request);
            Benchmarks.validate(result.size() == numCells,
                    "expected %s cells, found %s cells", numCells, result.size());
            return result;
        });
    }

    private SortedMap<byte[], RowResult<byte[]>> getRowsInner(ConsecutiveNarrowTable table, int numRows) {
        return table.getTransactionManager().runTaskThrowOnConflict(txn -> {
            Iterable<byte[]> request = table.getRowsRequest(numRows);
            SortedMap<byte[], RowResult<byte[]>> result =
                    txn.getRows(ConsecutiveNarrowTable.TABLE_REF, request, ColumnSelection.all());
            Benchmarks.validate(result.size() == numRows,
                    "expected %s cells, found %s cells", numRows, result.size());
            return result;
        });
    }

    private List<RowResult<byte[]>> getSingleRowWithRangeQueryInner(final ConsecutiveNarrowTable table) {
        return table.getTransactionManager().runTaskThrowOnConflict(txn -> {
            RangeRequest request = table.getRangeRequest(1);
            List<RowResult<byte[]>> result = BatchingVisitables.copyToList(
                    txn.getRange(ConsecutiveNarrowTable.TABLE_REF, request));
            byte[] rowName = Iterables.getOnlyElement(result).getRowName();
            int rowNumber = ConsecutiveNarrowTable.rowNumber(rowName);
            int expectedRowNumber = ConsecutiveNarrowTable.rowNumber(request.getStartInclusive());
            Benchmarks.validate(rowNumber == expectedRowNumber,
                    "Start Row %s, row number %s", expectedRowNumber, rowNumber);
            return result;
        });
    }

    private List<RowResult<byte[]>> getRangeInner(ConsecutiveNarrowTable table) {
        final int rangeRequestSize = (int) (0.1 * table.getNumRows());
        return table.getTransactionManager().runTaskThrowOnConflict(txn -> {
            RangeRequest request = table.getRangeRequest(rangeRequestSize);
            List<RowResult<byte[]>> results = BatchingVisitables.copyToList(txn.getRange(
                    ConsecutiveNarrowTable.TABLE_REF, request));
            Benchmarks.validate(results.size() == rangeRequestSize,
                    "Expected %s rows, found %s rows", rangeRequestSize, results.size());
            return results;
        });
    }

    private Iterable<BatchingVisitable<RowResult<byte[]>>> getRangesInner(ConsecutiveNarrowTable table) {
        return table.getTransactionManager().runTaskThrowOnConflict(txn -> {
            List<RangeRequest> requests = Stream
                    .generate(() -> table.getRangeRequest(RANGES_SINGLE_REQUEST_SIZE))
                    .limit((long) (table.getNumRows() * 0.1))
                    .collect(Collectors.toList());
            Iterable<BatchingVisitable<RowResult<byte[]>>> results =
                    txn.getRanges(ConsecutiveNarrowTable.TABLE_REF, requests);
            results.forEach(bvs -> {
                List<RowResult<byte[]>> result = BatchingVisitables.copyToList(bvs);
                Benchmarks.validate(result.size() == RANGES_SINGLE_REQUEST_SIZE,
                        "Expected %s rows, found %s rows", RANGES_SINGLE_REQUEST_SIZE, result.size());
            });
            return results;
        });
    }


    @Benchmark
    public Map<Cell, byte[]> getCell(ConsecutiveNarrowTable.CleanNarrowTable table) {
        return getCellsInner(table, 1);
    }

    @Benchmark
    public Map<Cell, byte[]> getCellDirty(ConsecutiveNarrowTable.DirtyNarrowTable table) {
        return getCellsInner(table, 1);
    }

    @Benchmark
    public Map<Cell, byte[]> getCellVeryDirty(ConsecutiveNarrowTable.VeryDirtyNarrowTable table) {
        return getCellsInner(table, 1);
    }


    @Benchmark
    public Map<Cell, byte[]> getCells(ConsecutiveNarrowTable.CleanNarrowTable table) {
        return getCellsInner(table, (int) (0.1 * table.getNumRows()));
    }

    @Benchmark
    public Map<Cell, byte[]> getCellsDirty(ConsecutiveNarrowTable.DirtyNarrowTable table) {
        return getCellsInner(table, (int) (0.1 * table.getNumRows()));
    }

    @Benchmark
    public Map<Cell, byte[]> getCellsVeryDirty(ConsecutiveNarrowTable.VeryDirtyNarrowTable table) {
        return getCellsInner(table, (int) (0.1 * table.getNumRows()));
    }


    @Benchmark
    public SortedMap<byte[], RowResult<byte[]>> getRow(ConsecutiveNarrowTable.CleanNarrowTable table) {
        return getRowsInner(table, 1);
    }

    @Benchmark
    public SortedMap<byte[], RowResult<byte[]>> getRowDirty(ConsecutiveNarrowTable.DirtyNarrowTable table) {
        return getRowsInner(table, 1);
    }

    @Benchmark
    public SortedMap<byte[], RowResult<byte[]>> getRowVeryDirty(ConsecutiveNarrowTable.VeryDirtyNarrowTable table) {
        return getRowsInner(table, 1);
    }


    @Benchmark
    public SortedMap<byte[], RowResult<byte[]>> getRows(ConsecutiveNarrowTable.CleanNarrowTable table) {
        return getRowsInner(table, (int) (0.1 * table.getNumRows()));
    }

    @Benchmark
    public SortedMap<byte[], RowResult<byte[]>> getRowsDirty(ConsecutiveNarrowTable.DirtyNarrowTable table) {
        return getRowsInner(table, (int) (0.1 * table.getNumRows()));
    }

    @Benchmark
    public SortedMap<byte[], RowResult<byte[]>> getRowsVeryDirty(ConsecutiveNarrowTable.VeryDirtyNarrowTable table) {
        return getRowsInner(table, (int) (0.1 * table.getNumRows()));
    }


    @Benchmark
    public  List<RowResult<byte[]>> getRowWithRangeQuery(ConsecutiveNarrowTable.CleanNarrowTable table) {
        return getSingleRowWithRangeQueryInner(table);
    }

    @Benchmark
    public  List<RowResult<byte[]>> getRowWithRangeQueryDirty(ConsecutiveNarrowTable.DirtyNarrowTable table) {
        return getSingleRowWithRangeQueryInner(table);
    }

    @Benchmark
    public  List<RowResult<byte[]>> getRowWithRangeQueryVeryDirty(
            ConsecutiveNarrowTable.VeryDirtyNarrowTable table) {
        return getSingleRowWithRangeQueryInner(table);
    }


    @Benchmark
    public List<RowResult<byte[]>> getRange(ConsecutiveNarrowTable.CleanNarrowTable table) {
        return getRangeInner(table);
    }

    @Benchmark
    public List<RowResult<byte[]>> getRangeDirty(ConsecutiveNarrowTable.DirtyNarrowTable table) {
        return getRangeInner(table);
    }

    @Benchmark
    public List<RowResult<byte[]>> getRangeVeryDirty(ConsecutiveNarrowTable.VeryDirtyNarrowTable table) {
        return getRangeInner(table);
    }


    @Benchmark
    public Iterable<BatchingVisitable<RowResult<byte[]>>> getRanges(ConsecutiveNarrowTable.CleanNarrowTable table) {
        return getRangesInner(table);
    }

    @Benchmark
    public Iterable<BatchingVisitable<RowResult<byte[]>>> getRangesDirty(
            ConsecutiveNarrowTable.DirtyNarrowTable table) {
        return getRangesInner(table);
    }

    @Benchmark
    public Iterable<BatchingVisitable<RowResult<byte[]>>> getRangesVeryDirty(
            ConsecutiveNarrowTable.VeryDirtyNarrowTable table) {
        return getRangesInner(table);
    }

}
