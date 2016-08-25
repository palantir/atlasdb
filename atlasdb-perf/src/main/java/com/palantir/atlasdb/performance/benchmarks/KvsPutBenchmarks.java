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

import java.nio.charset.StandardCharsets;
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
import com.palantir.atlasdb.performance.backend.AtlasDbServicesConnector;

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
public class KvsPutBenchmarks {

    private static final TableReference TABLE_REF_1 = TableReference.createFromFullyQualifiedName("performance.table1");
    private static final TableReference TABLE_REF_2 = TableReference.createFromFullyQualifiedName("performance.table2");
    private static final String ROW_COMPONENT = "key";
    private static final String COLUMN_NAME = "value";
    private static final byte [] COLUMN_NAME_IN_BYTES = COLUMN_NAME.getBytes(StandardCharsets.UTF_8);
    private static final long DUMMY_TIMESTAMP = 1L;

    private static final int VALUE_BYTE_ARRAY_SIZE = 100;
    private static final int KEY_BYTE_ARRAY_SIZE = 32;
    private static final long VALUE_SEED = 279L;
    private static final int BATCH_SIZE = 250;

    private AtlasDbServicesConnector connector;
    private KeyValueService kvs;
    private Random random = new Random(VALUE_SEED);

    @Setup
    public void setup(AtlasDbServicesConnector conn) {
        this.connector = conn;
        this.kvs = conn.connect().getKeyValueService();
        Benchmarks.createTable(kvs, TABLE_REF_1, ROW_COMPONENT, COLUMN_NAME);
        Benchmarks.createTable(kvs, TABLE_REF_2, ROW_COMPONENT, COLUMN_NAME);
    }

    @TearDown
    public void cleanup() throws Exception {
        this.kvs.dropTables(Sets.newHashSet(TABLE_REF_1, TABLE_REF_2));
        this.kvs.close();
        this.connector.close();
    }

    @Benchmark
    public void singleRandomPut() {
        byte[] value = generateValue();
        kvs.put(TABLE_REF_1, ImmutableMap.of(Cell.create(generateKey(), COLUMN_NAME_IN_BYTES), value), DUMMY_TIMESTAMP);
    }

    @Benchmark
    public void batchRandomPut() {
        kvs.put(TABLE_REF_1, generateBatch(BATCH_SIZE), DUMMY_TIMESTAMP);
    }

    @Benchmark
    public void batchRandomMultiPut() {
        Map<TableReference, Map<Cell, byte[]>> multiPutMap = Maps.newHashMap();
        multiPutMap.put(TABLE_REF_1, generateBatch(BATCH_SIZE));
        multiPutMap.put(TABLE_REF_2, generateBatch(BATCH_SIZE));
        kvs.multiPut(multiPutMap, DUMMY_TIMESTAMP);
    }

    private byte[] generateValue() {
        byte[] value = new byte[VALUE_BYTE_ARRAY_SIZE];
        random.nextBytes(value);
        return value;
    }

    private byte[] generateKey() {
        byte[] key = new byte[KEY_BYTE_ARRAY_SIZE];
        random.nextBytes(key);
        return key;
    }

    private Map<Cell, byte[]> generateBatch(int size) {
        Map<Cell, byte[]> map = Maps.newHashMapWithExpectedSize(size);
        for (int j = 0; j < size; j++) {
            byte[] key = generateKey();
            byte[] value = generateValue();
            map.put(Cell.create(key, COLUMN_NAME_IN_BYTES), value);
        }
        return map;
    }

}
