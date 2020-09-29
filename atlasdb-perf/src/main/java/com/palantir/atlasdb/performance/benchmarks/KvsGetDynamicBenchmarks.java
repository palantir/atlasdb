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
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.performance.benchmarks.table.ModeratelyWideRowTable;
import com.palantir.atlasdb.performance.benchmarks.table.Tables;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Performance benchmarks for KVS get with dynamic columns.
 *
 * @author coda
 *
 */
@State(Scope.Benchmark)
public class KvsGetDynamicBenchmarks {

    @Benchmark
    @Threads(1)
    @Warmup(time = 20, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 180, timeUnit = TimeUnit.SECONDS)
    public Object getAllColumnsExplicitly(ModeratelyWideRowTable table) {
        Map<Cell, Value> result = table.getKvs().get(table.getTableRef(), table.getAllCellsAtMaxTimestamp());
        Preconditions.checkState(result.size() == table.getNumCols(),
                "Should be %s columns, but were: %s", table.getNumCols(), result.size());
        return result;
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 6, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 60, timeUnit = TimeUnit.SECONDS)
    public Object getAllColumnsImplicitly(ModeratelyWideRowTable table) throws UnsupportedEncodingException {
        Map<Cell, Value> result = table.getKvs().getRows(
                table.getTableRef(),
                Collections.singleton(Tables.ROW_BYTES.array()),
                ColumnSelection.all(),
                Long.MAX_VALUE);
        Preconditions.checkState(result.size() == table.getNumCols(),
                "Should be %s columns, but were: %s", table.getNumCols(), result.size());
        return result;
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 1, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 5, timeUnit = TimeUnit.SECONDS)
    public Object getFirstColumnExplicitly(ModeratelyWideRowTable table) {
        Map<Cell, Value> result = table.getKvs().get(table.getTableRef(), table.getFirstCellAtMaxTimestampAsMap());
        Preconditions.checkState(result.size() == 1, "Should be %s column, but were: %s", 1, result.size());
        int value = Ints.fromByteArray(Iterables.getOnlyElement(result.values()).getContents());
        Preconditions.checkState(value == 0, "Value should be %s but is %s", 0,  value);
        return result;
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 1, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 5, timeUnit = TimeUnit.SECONDS)
    public Object getFirstColumnExplicitlyGetRows(ModeratelyWideRowTable table) throws UnsupportedEncodingException {
        Map<Cell, Value> result = table.getKvs()
                .getRows(table.getTableRef(), Collections.singleton(Tables.ROW_BYTES.array()),
                        ColumnSelection.create(
                                table.getFirstCellAsSet().stream().map(Cell::getColumnName).collect(Collectors.toList())
                        ),
                        Long.MAX_VALUE);
        Preconditions.checkState(result.size() == 1, "Should be %s column, but were: %s", 1, result.size());
        int value = Ints.fromByteArray(Iterables.getOnlyElement(result.values()).getContents());
        Preconditions.checkState(value == 0, "Value should be %s but is %s", 0,  value);
        return result;
    }

}
