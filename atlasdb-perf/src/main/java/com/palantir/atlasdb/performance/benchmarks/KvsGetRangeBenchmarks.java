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
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.performance.benchmarks.table.ConsecutiveNarrowTable;
import com.palantir.common.base.ClosableIterator;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

@State(Scope.Benchmark)
public class KvsGetRangeBenchmarks {

    private Object getSingleRangeInner(ConsecutiveNarrowTable table, int sliceSize) {
        RangeRequest request = Iterables.getOnlyElement(table.getRangeRequests(1, sliceSize));
        int startRow = Ints.fromByteArray(request.getStartInclusive());
        ClosableIterator<RowResult<Value>> result =
                table.getKvs().getRange(table.getTableRef(), request, Long.MAX_VALUE);
        ArrayList<RowResult<Value>> list = Lists.newArrayList(result);
        result.close();
        Preconditions.checkState(list.size() == sliceSize, "List size %s != %s", sliceSize, list.size());
        list.forEach(rowResult -> {
            byte[] rowName = rowResult.getRowName();
            int rowNumber = Ints.fromByteArray(rowName);
            Preconditions.checkState(rowNumber - startRow < sliceSize, "Start Row %s, row number %s, sliceSize %s",
                    startRow, rowNumber, sliceSize);
        });
        return result;
    }

    private Object getMultiRangeInner(ConsecutiveNarrowTable table) {
        Iterable<RangeRequest> requests = table.getRangeRequests((int) (table.getNumRows() * 0.1), 1);
        Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> results =
                table.getKvs().getFirstBatchForRanges(table.getTableRef(), requests, Long.MAX_VALUE);

        int numRequests = Iterables.size(requests);

        Preconditions.checkState(numRequests == results.size(),
                "Got %s requests and %s results, requests %s, results %s",
                numRequests, results.size(), requests, results);

        results.forEach((request, result) -> {
            Preconditions.checkState(1 == result.getResults().size(), "Key %s, List size is %s",
                    Ints.fromByteArray(request.getStartInclusive()), result.getResults().size());
            Preconditions.checkState(!result.moreResultsAvailable(), "Key %s, result.moreResultsAvailable() %s",
                    Ints.fromByteArray(request.getStartInclusive()), result.moreResultsAvailable());
            RowResult<Value> row = Iterables.getOnlyElement(result.getResults());
            Preconditions.checkState(Arrays.equals(request.getStartInclusive(), row.getRowName()),
                    "Request row is %s, result is %s",
                    Ints.fromByteArray(request.getStartInclusive()),
                    Ints.fromByteArray(row.getRowName()));
        });
        return results;
    }


    @Benchmark
    @Warmup(time = 1, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 5, timeUnit = TimeUnit.SECONDS)
    public Object getSingleRange(ConsecutiveNarrowTable.CleanNarrowTable table) {
        return getSingleRangeInner(table, 1);
    }

    @Benchmark
    @Warmup(time = 1, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 5, timeUnit = TimeUnit.SECONDS)
    public Object getSingleRangeDirty(ConsecutiveNarrowTable.DirtyNarrowTable table) {
        return getSingleRangeInner(table, 1);
    }


    @Benchmark
    @Warmup(time = 2, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 10, timeUnit = TimeUnit.SECONDS)
    public Object getSingleLargeRange(ConsecutiveNarrowTable.CleanNarrowTable table) {
        return getSingleRangeInner(table, (int) (0.1 * table.getNumRows()));
    }

    @Benchmark
    @Warmup(time = 20, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 120, timeUnit = TimeUnit.SECONDS)
    public Object getSingleLargeRangeDirty(ConsecutiveNarrowTable.DirtyNarrowTable table) {
        return getSingleRangeInner(table, (int) (0.1 * table.getNumRows()));
    }


    @Benchmark
    @Warmup(time = 5, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 30, timeUnit = TimeUnit.SECONDS)
    public Object getMultiRange(ConsecutiveNarrowTable.CleanNarrowTable table) {
        return getMultiRangeInner(table);
    }

    @Benchmark
    @Warmup(time = 10, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 60, timeUnit = TimeUnit.SECONDS)
    public Object getMultiRangeDirty(ConsecutiveNarrowTable.DirtyNarrowTable table) {
        return getMultiRangeInner(table);
    }

}
