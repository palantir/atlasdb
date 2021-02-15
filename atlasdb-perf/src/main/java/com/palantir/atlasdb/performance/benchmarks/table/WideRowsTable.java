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

import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.performance.backend.AtlasDbServicesConnector;
import com.palantir.atlasdb.performance.benchmarks.Benchmarks;
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/**
 * State class for creating a single Atlas table with one wide row.
 */
@State(Scope.Benchmark)
public class WideRowsTable {
    public static final int NUM_ROWS = 10000;
    public static final int NUM_COLS_PER_ROW = 20;

    private AtlasDbServicesConnector connector;
    private AtlasDbServices services;

    private TableReference tableRef;

    public TransactionManager getTransactionManager() {
        return services.getTransactionManager();
    }

    public KeyValueService getKvs() {
        return services.getKeyValueService();
    }

    public TableReference getTableRef() {
        return Tables.TABLE_REF;
    }

    @Setup
    public void setup(AtlasDbServicesConnector conn) {
        this.connector = conn;
        services = conn.connect();
        tableRef = Benchmarks.createTableWithDynamicColumns(
                services.getKeyValueService(), getTableRef(), Tables.ROW_COMPONENT, Tables.COLUMN_COMPONENT);
        storeData();
    }

    @TearDown
    public void cleanup() throws Exception {
        services.getKeyValueService().dropTables(Sets.newHashSet(tableRef));
        connector.close();
    }

    private void storeData() {
        services.getTransactionManager().runTaskThrowOnConflict(txn -> {
            Map<Cell, byte[]> values = new HashMap<>(NUM_ROWS * NUM_COLS_PER_ROW);
            for (int i = 0; i < NUM_ROWS; i++) {
                for (int j = 0; j < NUM_COLS_PER_ROW; j++) {
                    values.put(cell(i, j), Ints.toByteArray(i * NUM_COLS_PER_ROW + j));
                }
            }
            txn.put(this.tableRef, values);
            return null;
        });
    }

    private static Cell cell(int rowIndex, int colIndex) {
        return Cell.create(getRow(rowIndex), getColumn(colIndex));
    }

    public static byte[] getRow(int rowIndex) {
        return ("row_" + rowIndex).getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] getColumn(int colIndex) {
        return ("col_" + colIndex).getBytes(StandardCharsets.UTF_8);
    }
}
