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
package com.palantir.atlasdb.performance.benchmarks.table;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.performance.backend.AtlasDbServicesConnector;
import com.palantir.atlasdb.performance.benchmarks.Benchmarks;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.atlasdb.sweep.SweepTaskRunner;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.LongStream;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/**
 * State class for creating a single Atlas table with one wide row.
 */
@State(Scope.Benchmark)
public abstract class RegeneratingTable<T> {

    public static final long CELL_VERSIONS = 10_000;
    private static final int BATCH_SIZE = 250;
    public static final int SWEEP_DUPLICATES = 10;
    private static final int SWEEP_BATCH_SIZE = 10;

    protected Random random = new Random(Tables.RANDOM_SEED);

    private AtlasDbServicesConnector connector;
    private AtlasDbServices services;

    public TransactionManager getTransactionManager() {
        return services.getTransactionManager();
    }

    public KeyValueService getKvs() {
        return services.getKeyValueService();
    }

    public TableReference getTableRef() {
        return Tables.TABLE_REF;
    }

    public SweepTaskRunner getSweepTaskRunner() {
        return services.getSweepTaskRunner();
    }

    @Setup(Level.Trial)
    public void setup(AtlasDbServicesConnector conn) {
        this.connector = conn;
        this.services = conn.connect();
        setupTable();
        setupTableData();
    }

    protected void setupTable() {
        Benchmarks.createTable(services.getKeyValueService(), getTableRef(), Tables.ROW_COMPONENT, Tables.COLUMN_NAME);
    }

    @TearDown(Level.Invocation)
    public abstract void setupTableData();

    public abstract T getTableCells();

    @TearDown(Level.Trial)
    public void cleanup() throws Exception {
        this.services.getKeyValueService().dropTable(getTableRef());
        this.connector.close();
    }

    @State(Scope.Benchmark)
    public static class VersionedCellRegeneratingTable extends RegeneratingTable<Cell> {
        private static final Cell cell = Cell.create(PtBytes.toBytes("r"), PtBytes.toBytes("c"));
        private static final byte[] value = PtBytes.toBytes("v");

        private static final Multimap<Cell, Value> allVersions = getVersions();

        @Override
        public void setupTableData() {
            getKvs().truncateTable(getTableRef());
            getKvs().putWithTimestamps(getTableRef(), allVersions);
        }

        @Override
        public Cell getTableCells() {
            return cell;
        }

        private static Multimap<Cell, Value> getVersions() {
            Multimap<Cell, Value> versions = ArrayListMultimap.create();
            LongStream.rangeClosed(1, CELL_VERSIONS)
                    .boxed()
                    .forEach(timestamp -> versions.put(cell, Value.create(value, timestamp)));
            return versions;
        }
    }

    @State(Scope.Benchmark)
    public static class KvsRowRegeneratingTable extends RegeneratingTable<Multimap<Cell, Long>> {
        private Multimap<Cell, Long> data;

        @Override
        public void setupTableData() {
            getKvs().truncateTable(getTableRef());
            Map<Cell, byte[]> batch = Tables.generateRandomBatch(random, 1);
            getKvs().put(getTableRef(), batch, Tables.DUMMY_TIMESTAMP);
            data = Multimaps.forMap(Maps.transformValues(batch, _$ -> Tables.DUMMY_TIMESTAMP));
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
            getKvs().truncateTable(getTableRef());
            Map<Cell, byte[]> batch = Tables.generateRandomBatch(random, BATCH_SIZE);
            getKvs().put(getTableRef(), batch, Tables.DUMMY_TIMESTAMP);
            data = Multimaps.forMap(Maps.transformValues(batch, _$ -> Tables.DUMMY_TIMESTAMP));
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
            getKvs().truncateTable(getTableRef());
            Map<Cell, byte[]> batch = Tables.generateRandomBatch(random, 1);
            getTransactionManager().runTaskThrowOnConflict(txn -> {
                txn.put(getTableRef(), batch);
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
    public static class TransactionBatchRegeneratingTable extends RegeneratingTable<Set<Cell>> {
        private Set<Cell> cells;

        @Override
        public void setupTableData() {
            getKvs().truncateTable(getTableRef());
            Map<Cell, byte[]> batch = Tables.generateRandomBatch(random, BATCH_SIZE);
            getTransactionManager().runTaskThrowOnConflict(txn -> {
                txn.put(getTableRef(), batch);
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
    public static class SweepRegeneratingTable extends RegeneratingTable<Set<Cell>> {

        @Override
        public void setupTableData() {
            getKvs().truncateTable(getTableRef());
            populateTable(1, 1, SWEEP_DUPLICATES);
        }

        protected void populateTable(int numberOfBatches, int batchSize, int numberOfDuplicates) {
            for (int i = 0; i < numberOfBatches; i++) {
                Map<Cell, byte[]> batch = Tables.generateRandomBatch(random, batchSize);
                for (int j = 0; j < numberOfDuplicates; j++) {
                    getTransactionManager().runTaskThrowOnConflict(txn -> {
                        txn.put(getTableRef(), batch);
                        return null;
                    });
                }
            }
        }

        @Override
        public Set<Cell> getTableCells() {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        protected void setupTable() {
            Benchmarks.createTable(
                    getKvs(),
                    getTableRef(),
                    Tables.ROW_COMPONENT,
                    Tables.COLUMN_NAME,
                    TableMetadataPersistence.SweepStrategy.THOROUGH);
        }
    }

    @State(Scope.Benchmark)
    public static class SweepBatchUniformMultipleRegeneratingTable extends SweepRegeneratingTable {

        @Override
        public void setupTableData() {
            getKvs().truncateTable(getTableRef());
            populateTable(1, SWEEP_BATCH_SIZE, SWEEP_DUPLICATES);
        }
    }

    @State(Scope.Benchmark)
    public static class SweepBatchNonUniformMultipleSeparateRegeneratingTable extends SweepRegeneratingTable {

        @Override
        public void setupTableData() {
            getKvs().truncateTable(getTableRef());
            populateTable(SWEEP_BATCH_SIZE, 1, SWEEP_DUPLICATES);
        }
    }
}
