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

import static java.util.Collections.singletonList;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.performance.benchmarks.table.ConsecutiveNarrowTable;
import com.palantir.atlasdb.performance.benchmarks.table.Tables;
import java.util.Map;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@State(Scope.Benchmark)
public class KvsGetRowsBenchmarks {
    @Benchmark
    @Threads(1)
    @Warmup(time = 5)
    @Measurement(time = 40)
    public Object getManyRowsWithGetRows(ConsecutiveNarrowTable.CleanNarrowTable table) {
        Map<Cell, Value> result = table.getKvs().getRows(
                table.getTableRef(),
                table.getRowList(),
                ColumnSelection.all(),
                Long.MAX_VALUE
        );
        Preconditions.checkState(result.size() == table.getRowList().size(),
                "Should be %s rows, but were: %s", table.getRowList().size(), result.size());
        return result;
    }

    private static final ColumnSelection SINGLE_COLUMN_SELECTION =
            ColumnSelection.create(singletonList(Tables.COLUMN_NAME_IN_BYTES.array()));

    @Benchmark
    @Threads(1)
    @Warmup(time = 5)
    @Measurement(time = 40)
    public Object getSingleRowWithGetRow(ConsecutiveNarrowTable.DirtyNarrowTable table) {
        Map<Cell, Value> result = table.getKvs().getRows(
                table.getTableRef(),
                singletonList(Iterables.getFirst(table.getRowList(), null)),
                SINGLE_COLUMN_SELECTION,
                Long.MAX_VALUE
        );
        return result;
    }
}
