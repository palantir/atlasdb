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
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Warmup;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.performance.benchmarks.table.ConsecutiveNarrowTable;
import com.palantir.common.base.ClosableIterator;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 1, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 1, time = 30, timeUnit = TimeUnit.SECONDS)
public class KvsGetRangeBenchmarks {

    private Object getSingleRangeInner(ConsecutiveNarrowTable table, int sliceSize) {
        RangeRequest request = Iterables.getOnlyElement(table.getRangeRequests(1, sliceSize));
        int startRow = Ints.fromByteArray(request.getStartInclusive());
        ClosableIterator<RowResult<Value>> result =
                table.getKvs().getRange(table.getTableRef(), request, Long.MAX_VALUE);
        ArrayList<RowResult<Value>> list = Lists.newArrayList(result);
        Benchmarks.validate(list.size() == sliceSize, "List size %s != %s", sliceSize, list.size());
        list.forEach(rowResult -> {
            byte[] rowName = rowResult.getRowName();
            int rowNumber = Ints.fromByteArray(rowName);
            Benchmarks.validate(rowNumber - startRow < sliceSize, "Start Row %s, row number %s, sliceSize %s",
                    startRow, rowNumber, sliceSize);
        });
        return result;
    }

    private Object getMultiRangeInner(ConsecutiveNarrowTable table) {
        Iterable<RangeRequest> requests = table.getRangeRequests((int) (table.getNumRows() * 0.1), 1);
        Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> results =
                table.getKvs().getFirstBatchForRanges(table.getTableRef(), requests, Long.MAX_VALUE);

        int numRequests = Iterables.size(requests);

        Benchmarks.validate(numRequests == results.size(),
                "Got %s requests and %s results, requests %s, results %s",
                numRequests, results.size(), requests, results);

        results.forEach((request, result) -> {
            Benchmarks.validate(1 == result.getResults().size(), "Key %s, List size is %s",
                    Ints.fromByteArray(request.getStartInclusive()), result.getResults().size());
            Benchmarks.validate(!result.moreResultsAvailable(), "Key %s, result.moreResultsAvailable() %s",
                    Ints.fromByteArray(request.getStartInclusive()), result.moreResultsAvailable());
            RowResult<Value> row = Iterables.getOnlyElement(result.getResults());
            Benchmarks.validate(Arrays.equals(request.getStartInclusive(), row.getRowName()),
                    "Request row is %s, result is %s",
                    Ints.fromByteArray(request.getStartInclusive()),
                    Ints.fromByteArray(row.getRowName()));
        });
        return results;
    }


    @Benchmark
    public Object getSingleRange(ConsecutiveNarrowTable.CleanNarrowTable table) {
        return getSingleRangeInner(table, 1);
    }

    @Benchmark
    public Object getSingleRangeDirty(ConsecutiveNarrowTable.DirtyNarrowTable table) {
        return getSingleRangeInner(table, 1);
    }

    @Benchmark
    public Object getSingleRangeVeryDirty(ConsecutiveNarrowTable.VeryDirtyNarrowTable table) {
        return getSingleRangeInner(table, 1);
    }


    @Benchmark
    public Object getSingleLargeRange(ConsecutiveNarrowTable.CleanNarrowTable table) {
        return getSingleRangeInner(table, (int) (0.1 * table.getNumRows()));
    }

    @Benchmark
    public Object getSingleLargeRangeDirty(ConsecutiveNarrowTable.DirtyNarrowTable table) {
        return getSingleRangeInner(table, (int) (0.1 * table.getNumRows()));
    }

    @Benchmark
    public Object getSingleLargeRangeVeryDirty(ConsecutiveNarrowTable.VeryDirtyNarrowTable table) {
        return getSingleRangeInner(table, (int) (0.1 * table.getNumRows()));
    }


    @Benchmark
    public Object getMultiRange(ConsecutiveNarrowTable.CleanNarrowTable table) {
        return getMultiRangeInner(table);
    }

    @Benchmark
    public Object getMultiRangeDirty(ConsecutiveNarrowTable.DirtyNarrowTable table) {
        return getMultiRangeInner(table);
    }

    @Benchmark
    public Object getMultiRangeVeryDirty(ConsecutiveNarrowTable.VeryDirtyNarrowTable table) {
        return getMultiRangeInner(table);
    }

}
