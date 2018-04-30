/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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

import java.util.Map;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.performance.benchmarks.table.ConsecutiveNarrowTable;

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
}
