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
import java.util.stream.IntStream;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.performance.backend.AtlasDbServicesConnector;
import com.palantir.atlasdb.performance.benchmarks.Benchmarks;
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.atlasdb.transaction.api.TransactionManager;

/**
 * State class for creating a single Atlas table and adding N rows with row names [0...N).
 * Benchmark classes should subclass and override {@linkplain #setupData()} to add more data.
 */
@State(Scope.Benchmark)
public abstract class ConsecutiveNarrowTable {

    public static final TableReference TABLE_REF = TableReference.createFromFullyQualifiedName("performance.table");
    private static final String ROW_COMPONENT = "key";
    private static final String COLUMN_NAME = "value";
    public static final ByteBuffer COLUMN_NAME_IN_BYTES = ByteBuffer.wrap(COLUMN_NAME.getBytes(StandardCharsets.UTF_8));

    private static final int VALUE_BYTE_ARRAY_SIZE = 100;
    private static final int PUT_BATCH_SIZE = 1000;
    private static final int DEFAULT_NUM_ROWS = 10000;

    private static final long VALUE_SEED = 279L;
    private Random random = new Random(VALUE_SEED);

    private AtlasDbServicesConnector connector;
    private AtlasDbServices services;

    public Random getRandom() {
        return random;
    }

    public TransactionManager getTransactionManager() {
        return services.getTransactionManager();
    }

    public KeyValueService getKvs() {
        return services.getKeyValueService();
    }

    public int getNumRows() {
        return DEFAULT_NUM_ROWS;
    }

    protected abstract void setupData();

    @TearDown(Level.Trial)
    public void cleanup() throws Exception {
        getKvs().dropTables(Sets.newHashSet(TABLE_REF));
        this.connector.close();
    }

    @Setup(Level.Trial)
    public void setup(AtlasDbServicesConnector conn) {
        this.connector = conn;
        services = conn.connect();
        Benchmarks.createTable(getKvs(), TABLE_REF, ROW_COMPONENT, COLUMN_NAME);
        setupData();
    }

    @State(Scope.Benchmark)
    public static class CleanNarrowTable extends ConsecutiveNarrowTable {
        @Override
        protected void setupData() {
            storeDataInTable(this, 1);
        }
    }

    @State(Scope.Benchmark)
    public static class DirtyNarrowTable extends ConsecutiveNarrowTable {
        @Override
        protected void setupData() {
            storeDataInTable(this, 10);
        }
    }

    @State(Scope.Benchmark)
    public static class VeryDirtyNarrowTable extends ConsecutiveNarrowTable {
        @Override
        protected void setupData() {
            storeDataInTable(this, 1000);
        }
        @Override
        public int getNumRows() {
            return 10;
        }
    }

    private static void storeDataInTable(ConsecutiveNarrowTable table, int numOverwrites) {
        int numRows = table.getNumRows();
        IntStream.range(0, numOverwrites + 1).forEach($ -> {
            for (int i = 0; i < numRows; i += PUT_BATCH_SIZE) {
                final Map<Cell, byte[]> values =
                        generateBatch(table.getRandom(), i, Math.min(PUT_BATCH_SIZE, numRows - i));
                table.getTransactionManager().runTaskThrowOnConflict(txn -> {
                    txn.put(TABLE_REF, values);
                    return null;
                });
            }
        });
    }

    private static Map<Cell, byte[]> generateBatch(Random random, int startKey, int size) {
        Map<Cell, byte[]> map = Maps.newHashMapWithExpectedSize(size);
        for (int j = 0; j < size; j++) {
            byte[] key = Ints.toByteArray(startKey + j);
            byte[] value = generateValue(random);
            map.put(Cell.create(key, COLUMN_NAME_IN_BYTES.array()), value);
        }
        return map;
    }

    private static byte[] generateValue(Random random) {
        byte[] value = new byte[VALUE_BYTE_ARRAY_SIZE];
        random.nextBytes(value);
        return value;
    }

}
