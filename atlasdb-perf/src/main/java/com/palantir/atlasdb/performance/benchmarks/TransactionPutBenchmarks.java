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
 */

package com.palantir.atlasdb.performance.benchmarks;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.performance.benchmarks.table.EmptyTables;

/**
 * Performance benchmarks for KVS put operations.
 *
 * @author mwakerman
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 1, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 1, time = 30, timeUnit = TimeUnit.SECONDS)
public class TransactionPutBenchmarks {

    private static final int BATCH_SIZE = 250;

    @Benchmark
    public Map<Cell, byte[]> singleRandomPut(EmptyTables tables) {
        Map<Cell, byte[]> batch = tables.generateBatchToInsert(1);
        tables.getTransactionManager().runTaskThrowOnConflict(txn -> {
            txn.put(EmptyTables.TABLE_REF_1, batch);
            return batch;
        });
        return tables.getTransactionManager().runTaskThrowOnConflict(txn -> {
            txn.delete(EmptyTables.TABLE_REF_1, batch.keySet());
            return batch;
        });
    }

    @Benchmark
    public Map<Cell, byte[]> batchRandomPut(EmptyTables tables) {
        Map<Cell, byte[]> batch = tables.generateBatchToInsert(BATCH_SIZE);
        tables.getTransactionManager().runTaskThrowOnConflict(txn -> {
            txn.put(EmptyTables.TABLE_REF_1, batch);
            return batch;
        });
        return tables.getTransactionManager().runTaskThrowOnConflict(txn -> {
            txn.delete(EmptyTables.TABLE_REF_1, batch.keySet());
            return batch;
        });
    }

}
