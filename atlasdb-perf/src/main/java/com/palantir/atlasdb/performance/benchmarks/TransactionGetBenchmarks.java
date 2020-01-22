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
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.performance.benchmarks.table.ConsecutiveNarrowTable;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.common.base.BatchingVisitables;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@State(Scope.Benchmark)
public class TransactionGetBenchmarks {

    private static final int RANGES_SINGLE_REQUEST_SIZE = 1;

    private Map<Cell, byte[]> getSingleCellInner(ConsecutiveNarrowTable table) {
        return table.getTransactionManager().runTaskThrowOnConflict(txn -> {
            Set<Cell> request = table.getCellsRequest(1);
            Map<Cell, byte[]> result = txn.get(table.getTableRef(), request);
            byte[] rowName = Iterables.getOnlyElement(result.entrySet()).getKey().getRowName();
            int rowNumber = Ints.fromByteArray(rowName);
            int expectRowNumber = ConsecutiveNarrowTable.rowNumber(Iterables.getOnlyElement(request).getRowName());
            Preconditions.checkState(rowNumber == expectRowNumber,
                    "Start Row %s, row number %s", expectRowNumber, rowNumber);
            return result;
        });
    }

    private Map<Cell, byte[]> getCellsInner(ConsecutiveNarrowTable table) {
        final int getCellsSize = 1000;
        return table.getTransactionManager().runTaskThrowOnConflict(txn -> {
            Set<Cell> request = table.getCellsRequest(getCellsSize);
            Map<Cell, byte[]> result = txn.get(table.getTableRef(), request);
            Preconditions.checkState(result.size() == getCellsSize,
                    "expected %s cells, found %s cells", getCellsSize, result.size());
            return result;
        });
    }

    private List<RowResult<byte[]>> getSingleRowWithRangeQueryInner(final ConsecutiveNarrowTable table) {
        return table.getTransactionManager().runTaskThrowOnConflict(txn -> {
            RangeRequest request = Iterables.getOnlyElement(table.getRangeRequests(1, 1, false));
            List<RowResult<byte[]>> result = BatchingVisitables.copyToList(
                    txn.getRange(table.getTableRef(), request));
            byte[] rowName = Iterables.getOnlyElement(result).getRowName();
            int rowNumber = ConsecutiveNarrowTable.rowNumber(rowName);
            int expectedRowNumber = ConsecutiveNarrowTable.rowNumber(request.getStartInclusive());
            Preconditions.checkState(rowNumber == expectedRowNumber,
                    "Start Row %s, row number %s", expectedRowNumber, rowNumber);
            return result;
        });
    }

    private List<RowResult<byte[]>> getRangeInner(ConsecutiveNarrowTable table) {
        final int rangeRequestSize = 1000;
        return table.getTransactionManager().runTaskThrowOnConflict(txn -> {
            RangeRequest request = Iterables.getOnlyElement(table.getRangeRequests(1, rangeRequestSize, false));
            List<RowResult<byte[]>> results = BatchingVisitables.copyToList(txn.getRange(
                    table.getTableRef(), request));
            Preconditions.checkState(results.size() == rangeRequestSize,
                    "Expected %s rows, found %s rows", rangeRequestSize, results.size());
            return results;
        });
    }

    private Iterable<BatchingVisitable<RowResult<byte[]>>> getRangesInner(ConsecutiveNarrowTable table) {
        return table.getTransactionManager().runTaskThrowOnConflict(txn -> {
            Iterable<RangeRequest> requests =
                    table.getRangeRequests(1000, RANGES_SINGLE_REQUEST_SIZE, false);
            Iterable<BatchingVisitable<RowResult<byte[]>>> results =
                    txn.getRanges(table.getTableRef(), requests);
            results.forEach(bvs -> {
                List<RowResult<byte[]>> result = BatchingVisitables.copyToList(bvs);
                Preconditions.checkState(result.size() == RANGES_SINGLE_REQUEST_SIZE,
                        "Expected %s rows, found %s rows", RANGES_SINGLE_REQUEST_SIZE, result.size());
            });
            return results;
        });
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 1, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 5, timeUnit = TimeUnit.SECONDS)
    public Object getCells(ConsecutiveNarrowTable.CleanNarrowTable table) {
        return getCellsInner(table);
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 2, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 10, timeUnit = TimeUnit.SECONDS)
    public Object getCellsDirty(ConsecutiveNarrowTable.DirtyNarrowTable table) {
        return getCellsInner(table);
    }


    @Benchmark
    @Threads(1)
    @Warmup(time = 1, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 5, timeUnit = TimeUnit.SECONDS)
    public Object getSingleRowWithRangeQuery(ConsecutiveNarrowTable.CleanNarrowTable table) {
        return getSingleRowWithRangeQueryInner(table);
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 1, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 5, timeUnit = TimeUnit.SECONDS)
    public Object getSingleRowWithRangeQueryDirty(ConsecutiveNarrowTable.DirtyNarrowTable table) {
        return getSingleRowWithRangeQueryInner(table);
    }


    @Benchmark
    @Threads(1)
    @Warmup(time = 2, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 10, timeUnit = TimeUnit.SECONDS)
    public Object getRange(ConsecutiveNarrowTable.CleanNarrowTable table) {
        return getRangeInner(table);
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 8, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 45, timeUnit = TimeUnit.SECONDS)
    public Object getRangeDirty(ConsecutiveNarrowTable.DirtyNarrowTable table) {
        return getRangeInner(table);
    }


    @Benchmark
    @Threads(1)
    @Warmup(time = 1, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 5, timeUnit = TimeUnit.SECONDS)
    public Object getSingleCell(ConsecutiveNarrowTable.CleanNarrowTable table) {
        return getSingleCellInner(table);
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 1, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 5, timeUnit = TimeUnit.SECONDS)
    public Object getSingleCellDirty(ConsecutiveNarrowTable.DirtyNarrowTable table) {
        return getSingleCellInner(table);
    }


    @Benchmark
    @Threads(1)
    @Warmup(time = 8, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 40, timeUnit = TimeUnit.SECONDS)
    public Object getRanges(ConsecutiveNarrowTable.CleanNarrowTable table) {
        return getRangesInner(table);
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 15, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 70, timeUnit = TimeUnit.SECONDS)
    public Object getRangesDirty(
            ConsecutiveNarrowTable.DirtyNarrowTable table) {
        return getRangesInner(table);
    }

}
