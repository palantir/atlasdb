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

import com.google.common.collect.ImmutableSet;
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
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@State(Scope.Benchmark)
public abstract class AbstractWideRowsTable {
    private final int numRows;
    private final int numColumnsPerRow;

    private AtlasDbServicesConnector connector;
    private AtlasDbServices services;

    protected AbstractWideRowsTable(int numRows, int numColumnsPerRow) {
        this.numRows = numRows;
        this.numColumnsPerRow = numColumnsPerRow;
    }

    public abstract TableReference getTableRef();

    public KeyValueService getKvs() {
        return services.getKeyValueService();
    }

    public TransactionManager getTransactionManager() {
        return services.getTransactionManager();
    }

    @TearDown(Level.Trial)
    public void cleanup() throws Exception {
        getKvs().dropTables(ImmutableSet.of(getTableRef()));
        connector.close();
    }

    @Setup(Level.Trial)
    public void setup(AtlasDbServicesConnector conn) {
        connector = conn;
        services = conn.connect();
        if (!services.getKeyValueService().getAllTableNames().contains(getTableRef())) {
            Benchmarks.createTable(getKvs(), getTableRef(), Tables.ROW_COMPONENT, Tables.COLUMN_NAME);
            storeData();
        }
    }

    private void storeData() {
        services.getTransactionManager().runTaskThrowOnConflict(txn -> {
            Map<Cell, byte[]> values = new HashMap<>(numRows * numColumnsPerRow);
            for (int i = 0; i < numRows; i++) {
                for (int j = 0; j < numColumnsPerRow; j++) {
                    values.put(cell(i, j), Ints.toByteArray(i * numColumnsPerRow + j));
                }
            }
            txn.put(getTableRef(), values);
            return null;
        });
    }

    protected static Cell cell(int rowIndex, int colIndex) {
        return Cell.create(getRow(rowIndex), getColumn(colIndex));
    }

    public static byte[] getRow(int rowIndex) {
        return ("row_" + rowIndex).getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] getColumn(int colIndex) {
        return ("col_" + colIndex).getBytes(StandardCharsets.UTF_8);
    }
}
