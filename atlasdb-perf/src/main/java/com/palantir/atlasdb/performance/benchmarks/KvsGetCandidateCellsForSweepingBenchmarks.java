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
import com.google.common.collect.Iterators;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.ImmutableCandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.performance.benchmarks.table.ConsecutiveNarrowTable;
import com.palantir.atlasdb.performance.benchmarks.table.VeryWideRowTable;
import com.palantir.common.base.ClosableIterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@State(Scope.Benchmark)
public class KvsGetCandidateCellsForSweepingBenchmarks {

    @Benchmark
    @Threads(1)
    @Warmup(time = 20, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 160, timeUnit = TimeUnit.SECONDS)
    public Object fullTableScanCleanConservative(ConsecutiveNarrowTable.CleanNarrowTable table) {
        return fullTableScan(table, false);
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 20, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 160, timeUnit = TimeUnit.SECONDS)
    public Object fullTableScanCleanThorough(ConsecutiveNarrowTable.CleanNarrowTable table) {
        return fullTableScan(table, true);
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 20, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 160, timeUnit = TimeUnit.SECONDS)
    public Object fullTableScanDirtyConservative(ConsecutiveNarrowTable.DirtyNarrowTable table) {
        return fullTableScan(table, false);
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 20, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 160, timeUnit = TimeUnit.SECONDS)
    public Object fullTableScanDirtyThorough(ConsecutiveNarrowTable.DirtyNarrowTable table) {
        return fullTableScan(table, true);
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 20, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 160, timeUnit = TimeUnit.SECONDS)
    public Object fullTableScanOneWideRowThorough(VeryWideRowTable table) {
        return fullTableScan(table.getTableRef(), table.getKvs(), table.getNumCols(), true);
    }

    private int fullTableScan(ConsecutiveNarrowTable table, boolean thorough) {
        // TODO(gsheasby): consider extracting a common interface for WideRowTable and ConsecutiveNarrowTable
        // to avoid unpacking here
        return fullTableScan(table.getTableRef(), table.getKvs(), table.getNumRows(), thorough);
    }

    private int fullTableScan(TableReference tableRef,
                              KeyValueService kvs,
                              int numCellsExpected,
                              boolean thorough) {
        CandidateCellForSweepingRequest request = ImmutableCandidateCellForSweepingRequest.builder()
                    .startRowInclusive(PtBytes.EMPTY_BYTE_ARRAY)
                    .batchSizeHint(1000)
                    .maxTimestampExclusive(Long.MAX_VALUE)
                    .shouldCheckIfLatestValueIsEmpty(thorough)
                    .shouldDeleteGarbageCollectionSentinels(thorough)
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
