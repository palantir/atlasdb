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

package com.palantir.atlasdb.performance.tests;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.performance.backend.KeyValueServiceConnector;

/**
 * Performance benchmarks for KVS put operations.
 *
 * @author mwakerman
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
public class KvsPutBenchmarks {

    private static final String TABLE_NAME_1 = "performance.table1";
    private static final String TABLE_NAME_2 = "performance.table2";
    private static final String ROW_COMPONENT = "key";
    private static final String COLUMN_NAME = "value";
    private static final byte [] COLUMN_NAME_IN_BYTES = COLUMN_NAME.getBytes();
    private static final long DUMMY_TIMESTAMP = 1L;

    private static final int VALUE_BYTE_ARRAY_SIZE = 100;
    private static final int KEY_BYTE_ARRAY_SIZE = 32;
    private static final long VALUE_SEED = 279L;
    private static final int BATCH_SIZE = 250;

    private KeyValueServiceConnector connector;
    private KeyValueService kvs;
    private Random random = new Random(VALUE_SEED);

    private TableReference tableRef1;
    private TableReference tableRef2;

    @Setup
    public void setup(KeyValueServiceConnector connector) {
        this.connector = connector;
        kvs = connector.connect();
        tableRef1 = KvsBenchmarks.createTable(kvs, TABLE_NAME_1, ROW_COMPONENT, COLUMN_NAME);
        tableRef2 = KvsBenchmarks.createTable(kvs, TABLE_NAME_2, ROW_COMPONENT, COLUMN_NAME);
    }

    @TearDown
    public void cleanup() throws Exception {
        kvs.dropTables(Sets.newHashSet(tableRef1, tableRef2));
        kvs.close();
        connector.close();
    }

    @Benchmark
    public void singleRandomPut() {
        byte[] key = new byte[KEY_BYTE_ARRAY_SIZE];
        byte[] value = new byte[VALUE_BYTE_ARRAY_SIZE];
        random.nextBytes(key);
        random.nextBytes(value);
        kvs.put(tableRef1, ImmutableMap.of(Cell.create(key, COLUMN_NAME_IN_BYTES), value), DUMMY_TIMESTAMP);
    }

    @Benchmark
    public void batchRandomPut() {
        kvs.put(tableRef1, createBatch(BATCH_SIZE), DUMMY_TIMESTAMP);
    }

    @Benchmark
    public void batchRandomMultiPut() {
        Map<TableReference, Map<Cell, byte[]>> multiPutMap = Maps.newHashMap();
        multiPutMap.put(tableRef1, createBatch(BATCH_SIZE));
        multiPutMap.put(tableRef2, createBatch(BATCH_SIZE));
        kvs.multiPut(multiPutMap, DUMMY_TIMESTAMP);
    }

    private Map<Cell, byte[]> createBatch(int size) {
        Map<Cell, byte[]> map = Maps.newHashMap();
        for (int j=0; j<size; j++) {
            byte[] key = new byte[KEY_BYTE_ARRAY_SIZE];
            byte[] value = new byte[VALUE_BYTE_ARRAY_SIZE];
            random.nextBytes(key);
            random.nextBytes(value);
            map.put(Cell.create(key, COLUMN_NAME_IN_BYTES), value);
        }
        return map;
    }

}
