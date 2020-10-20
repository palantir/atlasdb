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
import com.palantir.atlasdb.performance.benchmarks.table.EmptyTables;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Performance benchmarks for KVS put operations.
 *
 * @author mwakerman
 */
@State(Scope.Benchmark)
public class TransactionPutBenchmarks {

    private static final int BATCH_SIZE = 250;

    @Benchmark
    @Threads(1)
    @Warmup(time = 2, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 10, timeUnit = TimeUnit.SECONDS)
    public Object singleRandomPut(EmptyTables tables) {
        return tables.getTransactionManager().runTaskThrowOnConflict(txn -> {
            Map<Cell, byte[]> batch = tables.generateBatchToInsert(1);
            txn.put(tables.getFirstTableRef(), batch);
            return batch;
        });
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 3, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 15, timeUnit = TimeUnit.SECONDS)
    public Object batchRandomPut(EmptyTables tables) {
        return tables.getTransactionManager().runTaskThrowOnConflict(txn -> {
            Map<Cell, byte[]> batch = tables.generateBatchToInsert(BATCH_SIZE);
            txn.put(tables.getFirstTableRef(), batch);
            return batch;
        });
    }
}
