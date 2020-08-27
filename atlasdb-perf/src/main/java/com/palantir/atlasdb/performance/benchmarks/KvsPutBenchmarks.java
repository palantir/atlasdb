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

import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.performance.benchmarks.table.EmptyTables;
import com.palantir.logsafe.Preconditions;
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
public class KvsPutBenchmarks {

    private static final long DUMMY_TIMESTAMP = 1L;
    private static final int BATCH_SIZE = 250;

    @Benchmark
    @Threads(1)
    @Warmup(time = 1, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 6, timeUnit = TimeUnit.SECONDS)
    public Object singleRandomPut(EmptyTables tables) {
        Map<Cell, byte[]> batch = tables.generateBatchToInsert(1);
        tables.getKvs().put(tables.getFirstTableRef(), batch, DUMMY_TIMESTAMP);
        return batch;
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 3, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 15, timeUnit = TimeUnit.SECONDS)
    public Object batchRandomPut(EmptyTables tables) {
        Map<Cell, byte[]> batch = tables.generateBatchToInsert(BATCH_SIZE);
        tables.getKvs().put(tables.getFirstTableRef(), batch, DUMMY_TIMESTAMP);
        return batch;
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 5, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 25, timeUnit = TimeUnit.SECONDS)
    public Object batchRandomMultiPut(EmptyTables tables) {
        Map<TableReference, Map<Cell, byte[]>> multiPutMap = Maps.newHashMap();
        multiPutMap.put(tables.getFirstTableRef(), tables.generateBatchToInsert(BATCH_SIZE));
        multiPutMap.put(tables.getSecondTableRef(), tables.generateBatchToInsert(BATCH_SIZE));
        tables.getKvs().multiPut(multiPutMap, DUMMY_TIMESTAMP);
        return multiPutMap;
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 2, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 20, timeUnit = TimeUnit.SECONDS)
    public Map<Cell, byte[]> putUnlessExistsAndExists(EmptyTables tables) {
        Map<Cell, byte[]> batch = tables.generateBatchToInsert(1);
        tables.getKvs().putUnlessExists(tables.getFirstTableRef(), batch);
        try {
            tables.getKvs().putUnlessExists(tables.getFirstTableRef(), batch);
            Preconditions.checkArgument(false, "putUnlessExists should have failed");
        } catch (KeyAlreadyExistsException e) {
            // success
        }
        return batch;
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 1, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 10, timeUnit = TimeUnit.SECONDS)
    public Map<Cell, byte[]> putUnlessExistsDoesNotExist(EmptyTables tables) {
        Map<Cell, byte[]> batch = tables.generateBatchToInsert(1);
        tables.getKvs().putUnlessExists(tables.getFirstTableRef(), batch);
        return batch;
    }

}
