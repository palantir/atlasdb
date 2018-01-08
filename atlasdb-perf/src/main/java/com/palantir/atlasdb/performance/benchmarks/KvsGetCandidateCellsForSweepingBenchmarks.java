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

import java.util.List;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.ImmutableCandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.performance.benchmarks.table.ConsecutiveNarrowTable;
import com.palantir.atlasdb.performance.benchmarks.table.VeryWideRowTable;
import com.palantir.common.base.ClosableIterator;

@State(Scope.Benchmark)
public class KvsGetCandidateCellsForSweepingBenchmarks {
    private static final int DEFAULT_BATCH_SIZE = 1000;

    // Batch sizes for (temporary?) tests
    private static final int TINY = 16; // 2^4
    private static final int SMALL = 128; // 2^7
    private static final int NORMAL_SIZE = DEFAULT_BATCH_SIZE; // close enough to 2^10
    private static final int LARGE = 4096; // 2^12
    private static final int HUGE = 16384; // 2^14

    @Benchmark
    @Threads(1)
    @Warmup(time = 20)
    @Measurement(time = 160)
    public Object fullTableScanCleanConservative_Tiny(ConsecutiveNarrowTable.CleanNarrowTable table) {
        return fullTableScan(table, false, TINY);
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 20)
    @Measurement(time = 160)
    public Object fullTableScanCleanConservative_Small(ConsecutiveNarrowTable.CleanNarrowTable table) {
        return fullTableScan(table, false, SMALL);
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 20)
    @Measurement(time = 160)
    public Object fullTableScanCleanConservative_NormalSize(ConsecutiveNarrowTable.CleanNarrowTable table) {
        return fullTableScan(table, false, NORMAL_SIZE);
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 20)
    @Measurement(time = 160)
    public Object fullTableScanCleanConservative_Large(ConsecutiveNarrowTable.CleanNarrowTable table) {
        return fullTableScan(table, false, LARGE);
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 20)
    @Measurement(time = 160)
    public Object fullTableScanCleanConservative_Huge(ConsecutiveNarrowTable.CleanNarrowTable table) {
        return fullTableScan(table, false, HUGE);
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 20)
    @Measurement(time = 160)
    public Object fullTableScanCleanThorough(ConsecutiveNarrowTable.CleanNarrowTable table) {
        return fullTableScan(table, true, DEFAULT_BATCH_SIZE);
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 20)
    @Measurement(time = 160)
    public Object fullTableScanDirtyConservative_Tiny(ConsecutiveNarrowTable.DirtyNarrowTable table) {
        return fullTableScan(table, false, TINY);
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 20)
    @Measurement(time = 160)
    public Object fullTableScanDirtyConservative_Small(ConsecutiveNarrowTable.DirtyNarrowTable table) {
        return fullTableScan(table, false, SMALL);
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 20)
    @Measurement(time = 160)
    public Object fullTableScanDirtyConservative_NormalSize(ConsecutiveNarrowTable.DirtyNarrowTable table) {
        return fullTableScan(table, false, NORMAL_SIZE);
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 20)
    @Measurement(time = 160)
    public Object fullTableScanDirtyConservative_Large(ConsecutiveNarrowTable.DirtyNarrowTable table) {
        return fullTableScan(table, false, LARGE);
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 20)
    @Measurement(time = 160)
    public Object fullTableScanDirtyConservative_Huge(ConsecutiveNarrowTable.DirtyNarrowTable table) {
        return fullTableScan(table, false, HUGE);
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 20)
    @Measurement(time = 160)
    public Object fullTableScanDirtyThorough(ConsecutiveNarrowTable.DirtyNarrowTable table) {
        return fullTableScan(table, true, DEFAULT_BATCH_SIZE);
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 20)
    @Measurement(time = 160)
    public Object fullTableScanOneWideRowThorough(VeryWideRowTable table) {
        return fullTableScan(table.getTableRef(), table.getKvs(), table.getNumCols(), true, DEFAULT_BATCH_SIZE);
    }

    private int fullTableScan(ConsecutiveNarrowTable table, boolean thorough, int batchSizeHint) {
        // TODO(gsheasby): consider extracting a common interface for WideRowTable and ConsecutiveNarrowTable
        // to avoid unpacking here
        return fullTableScan(table.getTableRef(), table.getKvs(), table.getNumRows(), thorough, batchSizeHint);
    }

    private int fullTableScan(TableReference tableRef, KeyValueService kvs, int numCellsExpected, boolean thorough,
            int batchSizeHint) {
        CandidateCellForSweepingRequest request = ImmutableCandidateCellForSweepingRequest.builder()
                    .startRowInclusive(PtBytes.EMPTY_BYTE_ARRAY)
                    .batchSizeHint(batchSizeHint)
                    .maxTimestampExclusive(Long.MAX_VALUE)
                    .shouldCheckIfLatestValueIsEmpty(thorough)
                    .timestampsToIgnore(thorough ? ImmutableSet.of() : ImmutableSet.of(Value.INVALID_VALUE_TIMESTAMP))
                    .build();
        try (ClosableIterator<List<CandidateCellForSweeping>> iter = kvs.getCandidateCellsForSweeping(
                    tableRef, request)) {
            int numCandidates = Iterators.size(Iterators.concat(Iterators.transform(iter, List::iterator)));
            Preconditions.checkState(numCandidates == numCellsExpected,
                    "Number of candidates %s != %s", numCandidates, numCellsExpected);
            return numCandidates;
        }
    }
}
