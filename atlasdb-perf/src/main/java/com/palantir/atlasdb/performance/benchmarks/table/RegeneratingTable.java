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
package com.palantir.atlasdb.performance.benchmarks.table;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.performance.backend.AtlasDbServicesConnector;
import com.palantir.atlasdb.performance.benchmarks.Benchmarks;
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.atlasdb.transaction.api.TransactionManager;

/**
 * State class for creating a single Atlas table with one wide row.
 */
@State(Scope.Benchmark)
public abstract class RegeneratingTable<T> {

    public static final TableReference TABLE_REF = TableReference.createFromFullyQualifiedName("performance.table");
    private static final String ROW_COMPONENT = "key";
    private static final String COLUMN_NAME = "value";
    private static final ByteBuffer COLUMN_NAME_IN_BYTES =
            ByteBuffer.wrap(RegeneratingTable.COLUMN_NAME.getBytes(StandardCharsets.UTF_8));

    private static final int VALUE_BYTE_ARRAY_SIZE = 100;
    private static final int KEY_BYTE_ARRAY_SIZE = 32;
    private static final int BATCH_SIZE = 250;

    private static final long DUMMY_TIMESTAMP = 1L;
    private static final long VALUE_SEED = 279L;
    private Random random = new Random(VALUE_SEED);


    private AtlasDbServicesConnector connector;
    private AtlasDbServices services;

    public TransactionManager getTransactionManager() {
        return services.getTransactionManager();
    }

    public KeyValueService getKvs() {
        return services.getKeyValueService();
    }

    @Setup(Level.Trial)
    public void setup(AtlasDbServicesConnector conn) {
        this.connector = conn;
        this.services = conn.connect();
        Benchmarks.createTable(services.getKeyValueService(), TABLE_REF, ROW_COMPONENT, COLUMN_NAME);
        setupTableData();
    }

    @TearDown(Level.Invocation)
    public abstract void setupTableData();

    public abstract T getTableCells();

    @TearDown(Level.Trial)
    public void cleanup() throws Exception {
        this.services.getKeyValueService().dropTable(TABLE_REF);
        this.connector.close();
    }

    @State(Scope.Benchmark)
    public static class KvsRowRegeneratingTable extends RegeneratingTable<Multimap<Cell, Long>> {
        private Multimap<Cell, Long> data;

        @Override
        public void setupTableData() {
            getKvs().truncateTable(TABLE_REF);
            Map<Cell, byte[]> batch = generateBatchToInsert(1);
            getKvs().put(TABLE_REF, batch, DUMMY_TIMESTAMP);
            data = Multimaps.forMap(Maps.transformValues(batch, $ -> DUMMY_TIMESTAMP));
        }

        @Override
        public Multimap<Cell, Long> getTableCells() {
            return data;
        }
    }

    @State(Scope.Benchmark)
    public static class KvsBatchRegeneratingTable extends RegeneratingTable<Multimap<Cell, Long>> {
        private Multimap<Cell, Long> data;

        @Override
        public void setupTableData() {
            getKvs().truncateTable(TABLE_REF);
            Map<Cell, byte[]> batch = generateBatchToInsert(BATCH_SIZE);
            getKvs().put(TABLE_REF, batch, DUMMY_TIMESTAMP);
            data = Multimaps.forMap(Maps.transformValues(batch, $ -> DUMMY_TIMESTAMP));
        }

        @Override
        public Multimap<Cell, Long> getTableCells() {
            return data;
        }
    }

    @State(Scope.Benchmark)
    public static class TransactionRowRegeneratingTable extends RegeneratingTable<Set<Cell>> {
        private Set<Cell> cells;

        @Override
        public void setupTableData() {
            getKvs().truncateTable(TABLE_REF);
            Map<Cell, byte[]> batch = generateBatchToInsert(BATCH_SIZE);
            getTransactionManager().runTaskThrowOnConflict(txn -> {
                txn.put(TABLE_REF, batch);
                return null;
            });
            cells = batch.keySet();
        }

        @Override
        public Set<Cell> getTableCells() {
            return cells;
        }
    }

    @State(Scope.Benchmark)
    public static class TransactionBatchRegeneratingTable extends RegeneratingTable {
        private Set<Cell> cells;

        @Override
        public void setupTableData() {
            getKvs().truncateTable(TABLE_REF);
            Map<Cell, byte[]> batch = generateBatchToInsert(BATCH_SIZE);
            getTransactionManager().runTaskThrowOnConflict(txn -> {
                txn.put(TABLE_REF, batch);
                return null;
            });
            cells = batch.keySet();
        }

        @Override
        public Set<Cell> getTableCells() {
            return cells;
        }
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

    protected Map<Cell, byte[]> generateBatchToInsert(int size) {
        Map<Cell, byte[]> map = Maps.newHashMapWithExpectedSize(size);
        for (int j = 0; j < size; j++) {
            byte[] key = generateKey();
            byte[] value = generateValue();
            map.put(Cell.create(key, COLUMN_NAME_IN_BYTES.array()), value);
        }
        return map;
    }

}
