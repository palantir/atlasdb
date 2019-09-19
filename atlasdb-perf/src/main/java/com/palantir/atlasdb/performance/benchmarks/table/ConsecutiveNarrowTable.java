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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.performance.backend.AtlasDbServicesConnector;
import com.palantir.atlasdb.performance.benchmarks.Benchmarks;
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/**
 * State class for creating a single Atlas table and adding N rows with row names [0...N).
 * Benchmark classes should subclass and override {@linkplain #setupData()} to add more data.
 */
@State(Scope.Benchmark)
public abstract class ConsecutiveNarrowTable {

    private static final int DIRTY_NUM_ROWS = 10000;
    private static final int CLEAN_NUM_ROWS = 1_000_000;
    private static final int REGENERATING_NUM_ROWS = 500;
    private static final List<byte[]> ROW_LIST = populateRowNames();
    private static final int DEFAULT_NUM_ROWS = 10_000;

    private Random random = new Random(Tables.RANDOM_SEED);
    private AtlasDbServicesConnector connector;

    private static List<byte[]> populateRowNames() {
        List<byte[]> list = new ArrayList<>();
        for (int j = 0; j < DEFAULT_NUM_ROWS; ++j) {
            list.add(Ints.toByteArray(j));
        }
        return list;
    }

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

    public abstract TableReference getTableRef();

    public abstract int getNumRows();

    public List<byte[]> getRowList() {
        return ROW_LIST;
    }

    protected abstract void setupData();

    @Setup(Level.Trial)
    public void setup(AtlasDbServicesConnector conn) {
        this.connector = conn;
        services = conn.connect();
        if (!services.getKeyValueService().getAllTableNames().contains(getTableRef())) {
            Benchmarks.createTable(getKvs(), getTableRef(), Tables.ROW_COMPONENT, Tables.COLUMN_NAME);
            setupData();
        }
    }

    @TearDown(Level.Trial)
    public void cleanup() throws Exception {
        this.connector.close();
    }

    @State(Scope.Benchmark)
    public static class CleanNarrowTable extends ConsecutiveNarrowTable {
        @Override
        public TableReference getTableRef() {
            return TableReference.createFromFullyQualifiedName("performance.persistent_table_clean");
        }

        @Override
        public int getNumRows() {
            return CLEAN_NUM_ROWS;
        }

        @Override
        protected void setupData() {
            storeDataInTable(this, 1);
        }
    }

    @State(Scope.Benchmark)
    public static class RegeneratingCleanNarrowTable extends CleanNarrowTable {
        @TearDown(Level.Invocation)
        public void regenerateTable() {
            getKvs().truncateTable(getTableRef());
            setupData();
        }

        @Override
        public TableReference getTableRef() {
            return TableReference.createFromFullyQualifiedName("performance.regenerating_table_clean");
        }

        @Override
        public void cleanup() throws Exception {
            getKvs().dropTable(getTableRef());
            super.cleanup();
        }

        @Override
        public int getNumRows() {
            return REGENERATING_NUM_ROWS;
        }
    }

    @State(Scope.Benchmark)
    public static class DirtyNarrowTable extends ConsecutiveNarrowTable {
        @Override
        public TableReference getTableRef() {
            return TableReference.createFromFullyQualifiedName("performance.persistent_table_dirty");
        }

        @Override
        public int getNumRows() {
            return DIRTY_NUM_ROWS;
        }

        @Override
        protected void setupData() {
            storeDataInTable(this, 100);
        }
    }

    public static int rowNumber(byte[] row) {
        return Ints.fromByteArray(row);
    }

    private static Cell cell(int index) {
        byte[] key = Ints.toByteArray(index);
        return Cell.create(key, Tables.COLUMN_NAME_IN_BYTES.array());
    }

    public Set<Cell> getCellsRequest(int numberOfCellsToRequest) {
        Preconditions.checkState(getNumRows() >= numberOfCellsToRequest,
                "Unable to request %s rows from a table that only has %s rows.",
                numberOfCellsToRequest, getNumRows());
        return getRandom()
                .ints(0, getNumRows())
                .distinct()
                .limit(numberOfCellsToRequest)
                .mapToObj(ConsecutiveNarrowTable::cell)
                .collect(Collectors.toSet());
    }

    public Iterable<RangeRequest> getRangeRequests(int numRequests, int sliceSize, boolean allColumns) {
        List<RangeRequest> requests = Lists.newArrayList();
        Set<Integer> used = Sets.newHashSet();
        for (int i = 0; i < numRequests; i++) {
            int startRow;
            do {
                startRow = getRandom().nextInt(getNumRows() - sliceSize);
            } while (used.contains(startRow));
            int endRow = startRow + sliceSize;
            RangeRequest request = RangeRequest.builder()
                    .batchHint(1 + sliceSize)
                    .startRowInclusive(Ints.toByteArray(startRow))
                    .endRowExclusive(Ints.toByteArray(endRow))
                    .retainColumns(allColumns
                            ? ColumnSelection.all()
                            : ColumnSelection.create(ImmutableList.of(Tables.COLUMN_NAME_IN_BYTES.array())))
                    .build();
            requests.add(request);
            used.add(startRow);
        }
        return requests;
    }

    private static void storeDataInTable(ConsecutiveNarrowTable table, int numOverwrites) {
        IntStream.range(0, numOverwrites + 1).forEach(
                $ -> table.getTransactionManager().runTaskThrowOnConflict(
                        txn -> {
                            Map<Cell, byte[]> values =
                                    Tables.generateContinuousBatch(table.getRandom(), 0, table.getNumRows());
                            txn.put(table.getTableRef(), values);
                            return null;
                        }));
    }
}
