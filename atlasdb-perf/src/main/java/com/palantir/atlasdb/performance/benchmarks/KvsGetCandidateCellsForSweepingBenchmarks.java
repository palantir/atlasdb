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
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.ImmutableCandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.performance.benchmarks.table.ConsecutiveNarrowTable;
import com.palantir.common.base.ClosableIterator;

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

    private int fullTableScan(ConsecutiveNarrowTable table, boolean thorough) {
        CandidateCellForSweepingRequest request = ImmutableCandidateCellForSweepingRequest.builder()
                    .startRowInclusive(PtBytes.EMPTY_BYTE_ARRAY)
                    .batchSizeHint(1000)
                    .minUncommittedStartTimestamp(Long.MIN_VALUE)
                    .sweepTimestamp(Long.MAX_VALUE)
                    .shouldCheckIfLatestValueIsEmpty(thorough)
                    .timestampsToIgnore(thorough ? new long[] {} : new long[] { Value.INVALID_VALUE_TIMESTAMP })
                    .build();
        try (ClosableIterator<List<CandidateCellForSweeping>> iter = table.getKvs().getCandidateCellsForSweeping(
                    table.getTableRef(), request)) {
            int numCandidates = Iterators.size(Iterators.concat(Iterators.transform(iter, List::iterator)));
            Preconditions.checkState(numCandidates == table.getNumRows(),
                    "Number of candidates %s != %s", numCandidates, table.getNumRows());
            return numCandidates;
        }
    }
}
