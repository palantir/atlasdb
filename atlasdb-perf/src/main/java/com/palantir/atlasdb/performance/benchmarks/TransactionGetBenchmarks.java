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
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.common.base.BatchingVisitables;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 1, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 1, time = 30, timeUnit = TimeUnit.SECONDS)
public class TransactionGetBenchmarks {

    private static final int GET_CELLS_SIZE = 500;
    private static final int RANGES_SINGLE_REQUEST_SIZE = 1;

    private Cell cell(int i) {
        byte[] key = Ints.toByteArray(i);
        return Cell.create(key, ConsecutiveNarrowTable.COLUMN_NAME_IN_BYTES);
    }

    private int rowNumber(byte[] row) {
        return Ints.fromByteArray(row);
    }

    private Set<Cell> getCellsRequest(ConsecutiveNarrowTable table, int numberOfCellsToRequest) {
        Set<Cell> ret = Sets.newHashSet();
        while (ret.size() < numberOfCellsToRequest) {
            ret.add(cell(table.getRandom().nextInt(table.getNumRows())));
        }
        return ret;
    }

    protected Map<Cell, byte[]> getSingleCellInner(ConsecutiveNarrowTable table) {
        return table.getTransactionManager().runTaskThrowOnConflict(txn -> {
            Set<Cell> request = getCellsRequest(table, 1);
            Map<Cell, byte[]> result = txn.get(table.getTableRef(), request);
            byte[] rowName = Iterables.getOnlyElement(result.entrySet()).getKey().getRowName();
            int rowNumber = Ints.fromByteArray(rowName);
            int expectRowNumber = rowNumber(Iterables.getOnlyElement(request).getRowName());
            Benchmarks.validate(rowNumber == expectRowNumber,
                    "Start Row %s, row number %s", expectRowNumber, rowNumber);
            return result;
        });
    }

    protected Map<Cell, byte[]> getCellsInner(ConsecutiveNarrowTable table) {
        return table.getTransactionManager().runTaskThrowOnConflict(txn -> {
            Set<Cell> request = getCellsRequest(table, GET_CELLS_SIZE);
            Map<Cell, byte[]> result = txn.get(table.getTableRef(), request);
            Benchmarks.validate(result.size() == GET_CELLS_SIZE,
                    "expected %s cells, found %s cells", GET_CELLS_SIZE, result.size());
            return result;
        });
    }

    private RangeRequest getRangeRequest(ConsecutiveNarrowTable table, int numberOfRowsToRequest) {
        int startRow = table.getRandom().nextInt(table.getNumRows() - numberOfRowsToRequest + 1);
        int endRow = startRow + numberOfRowsToRequest;
        return RangeRequest.builder()
                .batchHint(numberOfRowsToRequest + 1)
                .startRowInclusive(Ints.toByteArray(startRow))
                .endRowExclusive(Ints.toByteArray(endRow))
                .build();
    }

    protected List<RowResult<byte[]>> getSingleCellWithRangeQueryInner(final ConsecutiveNarrowTable table) {
        return table.getTransactionManager().runTaskThrowOnConflict(txn -> {
            RangeRequest request = getRangeRequest(table, 1);
            List<RowResult<byte[]>> result = BatchingVisitables.copyToList(txn.getRange(table.getTableRef(), request));
            byte[] rowName = Iterables.getOnlyElement(result).getRowName();
            int rowNumber = rowNumber(rowName);
            int expectedRowNumber = rowNumber(request.getStartInclusive());
            Benchmarks.validate(rowNumber == expectedRowNumber,
                    "Start Row %s, row number %s", expectedRowNumber, rowNumber);
            return result;
        });
    }

    protected List<RowResult<byte[]>> getRangeInner(ConsecutiveNarrowTable table) {
        final int rangeRequestSize = (int) (0.1 * table.getNumRows());
        return table.getTransactionManager().runTaskThrowOnConflict(txn -> {
            RangeRequest request = getRangeRequest(table, rangeRequestSize);
            List<RowResult<byte[]>> results = BatchingVisitables.copyToList(txn.getRange(
                    table.getTableRef(), request));
            Benchmarks.validate(results.size() == rangeRequestSize,
                    "Expected %s rows, found %s rows", rangeRequestSize, results.size());
            return results;
        });
    }

    protected Iterable<BatchingVisitable<RowResult<byte[]>>> getRangesInner(ConsecutiveNarrowTable table) {
        return table.getTransactionManager().runTaskThrowOnConflict(txn -> {
            List<RangeRequest> requests = Stream
                    .generate(() -> getRangeRequest(table, RANGES_SINGLE_REQUEST_SIZE))
                    .limit((long) (table.getNumRows() * 0.1))
                    .collect(Collectors.toList());
            Iterable<BatchingVisitable<RowResult<byte[]>>> results = txn.getRanges(table.getTableRef(), requests);
            results.forEach(bvs -> {
                List<RowResult<byte[]>> result = BatchingVisitables.copyToList(bvs);
                Benchmarks.validate(result.size() == RANGES_SINGLE_REQUEST_SIZE,
                        "Expected %s rows, found %s rows", RANGES_SINGLE_REQUEST_SIZE, result.size());
            });
            return results;
        });
    }

    @Benchmark
    public Map<Cell, byte[]> getCells(ConsecutiveNarrowTable.CleanNarrowTable table) {
        return getCellsInner(table);
    }

    @Benchmark
    public Map<Cell, byte[]> getCellsDirty(ConsecutiveNarrowTable.DirtyNarrowTable table) {
        return getCellsInner(table);
    }

    @Benchmark
    public Map<Cell, byte[]> getCellsVeryDirty(ConsecutiveNarrowTable.VeryDirtyNarrowTable table) {
        return getCellsInner(table);
    }


    @Benchmark
    public  List<RowResult<byte[]>> getSingleCellWithRangeQuery(ConsecutiveNarrowTable.CleanNarrowTable table) {
        return getSingleCellWithRangeQueryInner(table);
    }

    @Benchmark
    public  List<RowResult<byte[]>> getSingleCellWithRangeQueryDirty(ConsecutiveNarrowTable.DirtyNarrowTable table) {
        return getSingleCellWithRangeQueryInner(table);
    }

    @Benchmark
    public  List<RowResult<byte[]>> getSingleCellWithRangeQueryVeryDirty(
            ConsecutiveNarrowTable.VeryDirtyNarrowTable table) {
        return getSingleCellWithRangeQueryInner(table);
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
    public Map<Cell, byte[]> getSingleCell(ConsecutiveNarrowTable.CleanNarrowTable table) {
        return getSingleCellInner(table);
    }

    @Benchmark
    public Map<Cell, byte[]> getSingleCellDirty(ConsecutiveNarrowTable.DirtyNarrowTable table) {
        return getSingleCellInner(table);
    }

    @Benchmark
    public Map<Cell, byte[]> getSingleCellVeryDirty(ConsecutiveNarrowTable.VeryDirtyNarrowTable table) {
        return getSingleCellInner(table);
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
