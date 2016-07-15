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
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutableDbKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutablePostgresDdlConfig;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.performance.cli.AtlasDbPerfCLI;
import com.palantir.nexus.db.pool.config.ImmutablePostgresConnectionConfig;

/**
 * Performance benchmarks for KVS put operations.
 *
 * @author mwakerman
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class KvsPushBenchmarks {

    static private final String TABLE_NAME_1 = "performance.table1";
    static private final String TABLE_NAME_2 = "performance.table2";
    static private final String ROW_COMPONENT = "key";
    static private final String COLUMN_NAME = "value";
    static private final byte [] COLUMN_NAME_IN_BYTES = COLUMN_NAME.getBytes();
    static private final long DUMMY_TIMESTAMP = 1L;

    // TODO: are all of the below numbers 'reasonable'?
    static private final int VALUE_BYTE_ARRAY_SIZE = 100;
    static private final int KEY_BYTE_ARRAY_SIZE = 32;
    static private final long VALUE_SEED = 279L;

    static private final int BATCH_SIZE = 250;

    KeyValueService kvs;
    TableReference tableRef1;
    TableReference tableRef2;
    Random random = new Random(VALUE_SEED);

    // Note: this is run before EACH benchmark. TODO: can this run per 'group'?
    @Setup
    public void prepare(AtlasDbPerfCLI.ThreadState state) {
        System.out.println("state.backend = " + state.backend);
        // TODO: refactor and allow for different physical stores.
        // POSTGRES
        ImmutablePostgresConnectionConfig connectionConfig = ImmutablePostgresConnectionConfig.builder()
                .dbName("postgres")
                .dbLogin("postgres")
                .dbPassword("palantir")
                .host("0.0.0.0")
                .port(5432)
                .build();

        // ORACLE TODO where the driver at?
//        ImmutableOracleConnectionConfig connectionConfig = ImmutableOracleConnectionConfig.builder()
//                .host("0.0.0.0")
//                .port(1521)
//                .sid("xe")
//                .dbLogin("system")
//                .dbPassword("oracle")
//                .build();

        ImmutableDbKeyValueServiceConfig conf = ImmutableDbKeyValueServiceConfig.builder()
                .connection(connectionConfig)
                .ddl(ImmutablePostgresDdlConfig.builder().build())
                .build();

//        kvs = ConnectionManagerAwareDbKvs.create(conf);

        // Experiments with other KVSs
        kvs = new InMemoryKeyValueService(true);
//        kvs = RocksDbKeyValueService.create("testdb");

        tableRef1 = TestUtils.createTable(kvs, TABLE_NAME_1, ROW_COMPONENT, COLUMN_NAME);
        tableRef2 = TestUtils.createTable(kvs, TABLE_NAME_2, ROW_COMPONENT, COLUMN_NAME);
    }

    @Benchmark
    @Warmup(iterations = 5)
    @Measurement(iterations = 5)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void singleRandomPuts() {
        byte[] key = new byte[KEY_BYTE_ARRAY_SIZE];
        byte[] value = new byte[VALUE_BYTE_ARRAY_SIZE];
        random.nextBytes(key);
        random.nextBytes(value);
        kvs.put(tableRef1, ImmutableMap.of(Cell.create(key, COLUMN_NAME_IN_BYTES), value), DUMMY_TIMESTAMP);
    }

    @Benchmark
    @Warmup(iterations = 5)
    @Measurement(iterations = 5)
    public void batchRandomPuts() {
        kvs.put(tableRef1, createBatch(BATCH_SIZE), DUMMY_TIMESTAMP);
    }

    @Benchmark
    @Warmup(iterations = 5)
    @Measurement(iterations = 5)
    public void batchRandomMultiPuts() {
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

    @TearDown
    public void check() {
        kvs.dropTables(Sets.newHashSet(tableRef1, tableRef2));
        kvs.close();
    }
}
