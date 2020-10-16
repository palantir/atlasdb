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

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.performance.benchmarks.table.RegeneratingTable;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@State(Scope.Benchmark)
public class TransactionDeleteBenchmarks {

    private Object doDelete(RegeneratingTable<Set<Cell>> table) {
        return table.getTransactionManager().runTaskThrowOnConflict(txn -> {
            txn.delete(table.getTableRef(), table.getTableCells());
            return table.getTableCells();
        });
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 2, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 10, timeUnit = TimeUnit.SECONDS)
    public Object singleDelete(RegeneratingTable.TransactionRowRegeneratingTable table) {
        return doDelete(table);
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 4, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 20, timeUnit = TimeUnit.SECONDS)
    public Object batchDelete(RegeneratingTable.TransactionBatchRegeneratingTable table) {
        return doDelete(table);
    }
}
