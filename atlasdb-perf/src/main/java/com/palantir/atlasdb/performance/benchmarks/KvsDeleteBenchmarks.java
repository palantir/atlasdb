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
 */

package com.palantir.atlasdb.performance.benchmarks;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.performance.benchmarks.table.ConsecutiveNarrowTable;
import com.palantir.atlasdb.performance.benchmarks.table.RegeneratingTable;

@State(Scope.Benchmark)
public class KvsDeleteBenchmarks {

    private Object doDelete(RegeneratingTable<Multimap<Cell, Long>> table) {
        table.getKvs().delete(table.getTableRef(), table.getTableCells());
        return table.getTableCells();
    }

    private Object doDeleteRange(ConsecutiveNarrowTable table, int numBatches) {
        Iterable<RangeRequest> rangeRequests =
                numBatches == 1
                        ? ImmutableList.of(RangeRequest.all())
                        : table.getRangeRequests(1, table.getNumRows() / numBatches, true);

        rangeRequests.forEach(rangeRequest -> table.getKvs().deleteRange(table.getTableRef(), rangeRequest));

        return rangeRequests;
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 1, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 6, timeUnit = TimeUnit.SECONDS)
    public Object singleDelete(RegeneratingTable.KvsRowRegeneratingTable table) {
        return doDelete(table);
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 3, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 22, timeUnit = TimeUnit.SECONDS)
    public Object batchDelete(RegeneratingTable.KvsBatchRegeneratingTable table) {
        return doDelete(table);
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 6, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 60, timeUnit = TimeUnit.SECONDS)
    public Object batchRangeDelete(ConsecutiveNarrowTable.RegeneratingCleanNarrowTable table) {
        return doDeleteRange(table, 4);
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 5, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 45, timeUnit = TimeUnit.SECONDS)
    public Object allRangeDelete(ConsecutiveNarrowTable.RegeneratingCleanNarrowTable table) {
        return doDeleteRange(table, 1);
    }

}
