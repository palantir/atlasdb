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

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Random;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.performance.backend.AtlasDbServicesConnector;
import com.palantir.atlasdb.services.AtlasDbServices;

/**
 * State class for creating a single Atlas table and adding N rows with row names [0...N).
 * Benchmark classes should subclass and override setup as follows:
 * 1. {@linkplain org.openjdk.jmh.annotations.Setup} {@linkplain Level} Trial
 * 2. Call {@linkplain #setup(AtlasDbServicesConnector)}
 * 3. Call {@linkplain #storeData()} as appropriate
 */
@State(Scope.Benchmark)
public class ConsecutiveNarrowTable {


    private static final String TABLE_NAME_1 = "performance.table1";
    private static final String ROW_COMPONENT = "key";
    private static final String COLUMN_NAME = "value";
    private static final byte [] COLUMN_NAME_IN_BYTES = COLUMN_NAME.getBytes(StandardCharsets.UTF_8);
    protected static final long MIN_STORE_TS = 1L;
    private static final int VALUE_BYTE_ARRAY_SIZE = 100;
    private static final long VALUE_SEED = 279L;
    private static final int PUT_BATCH_SIZE = 1000;
    private AtlasDbServicesConnector connector;
    private KeyValueService kvs;

    private Random random = new Random(VALUE_SEED);
    private TableReference tableRef1;
    private final int numRows = 10000;
    private AtlasDbServices services;


    public AtlasDbServicesConnector getConnector() {
        return connector;
    }

    public KeyValueService getKvs() {
        return kvs;
    }

    public Random getRandom() {
        return random;
    }

    public TableReference getTableRef1() {
        return tableRef1;
    }

    public int getNumRows() {
        return numRows;
    }

    @TearDown(Level.Trial)
    public void cleanup() throws Exception {
        this.kvs.dropTables(Sets.newHashSet(tableRef1));
        this.connector.close();
        this.tableRef1 = null;
    }

    public void setup(AtlasDbServicesConnector conn) {
        this.connector = conn;
        services = conn.connect();
        this.kvs = services.getKeyValueService();
        this.tableRef1 = KvsBenchmarks.createTable(kvs, TABLE_NAME_1, ROW_COMPONENT, COLUMN_NAME);
    }

    protected void storeData() {
        for (int i = 0; i < numRows; i += PUT_BATCH_SIZE) {
            final Map<Cell, byte[]> values = generateBatch(i, Math.min(PUT_BATCH_SIZE, numRows - i));
            services.getTransactionManager().runTaskThrowOnConflict(txn -> {
                txn.put(tableRef1, values);
                return null;
            });
        }
    }

    private byte[] generateValue() {
        byte[] value = new byte[VALUE_BYTE_ARRAY_SIZE];
        random.nextBytes(value);
        return value;
    }

    private Map<Cell, byte[]> generateBatch(int startKey, int size) {
        Map<Cell, byte[]> map = Maps.newHashMapWithExpectedSize(size);
        for (int j = 0; j < size; j++) {
            byte[] key = Ints.toByteArray(startKey + j);
            byte[] value = generateValue();
            map.put(Cell.create(key, COLUMN_NAME_IN_BYTES), value);
        }
        return map;
    }
}
