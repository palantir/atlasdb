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
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;

/**
 * State class for creating a single Atlas table with one wide row.
 */
public abstract class WideRowTable {
    protected AtlasDbServicesConnector connector;
    protected AtlasDbServices services;

    protected TableReference tableRef;

    protected Map<Cell, Long> allCellsAtMaxTimestamp;
    protected Map<Cell, Long> firstCellAtMaxTimestamp;

    public TransactionManager getTransactionManager() {
        return services.getTransactionManager();
    }

    public KeyValueService getKvs() {
        return services.getKeyValueService();
    }

    public abstract TableReference getTableRef();

    public Map<Cell, Long> getAllCellsAtMaxTimestamp() {
        return allCellsAtMaxTimestamp;
    }

    public Map<Cell, Long> getFirstCellAtMaxTimestampAsMap() {
        return firstCellAtMaxTimestamp;
    }

    public Set<Cell> getAllCells() {
        return allCellsAtMaxTimestamp.keySet();
    }

    public Set<Cell> getFirstCellAsSet() {
        return firstCellAtMaxTimestamp.keySet();
    }

    public abstract int getNumCols();

    public abstract boolean isPersistent();

    @Setup
    public void setup(AtlasDbServicesConnector conn) throws UnsupportedEncodingException {
        this.connector = conn;
        services = conn.connect();
        if (!services.getKeyValueService().getAllTableNames().contains(getTableRef())) {
            tableRef = Benchmarks.createTableWithDynamicColumns(
                    services.getKeyValueService(),
                    getTableRef(),
                    Tables.ROW_COMPONENT,
                    Tables.COLUMN_COMPONENT);
            storeData();
        }
    }

    @TearDown
    public void cleanup() throws Exception {
        if (!isPersistent()) {
            services.getKeyValueService().dropTables(Sets.newHashSet(tableRef));
        }
        connector.close();
    }

    protected void storeData() {
        services.getTransactionManager().runTaskThrowOnConflict(txn -> {
            Map<Cell, byte[]> values = Maps.newHashMap();
            allCellsAtMaxTimestamp = Maps.newHashMap();
            firstCellAtMaxTimestamp = Maps.newHashMap();
            firstCellAtMaxTimestamp.put(cell(0), Long.MAX_VALUE);
            for (int i = 0; i < getNumCols(); i++) {
                Cell curCell = cell(i);
                values.put(curCell, Ints.toByteArray(i));
                allCellsAtMaxTimestamp.put(curCell, Long.MAX_VALUE);
            }
            txn.put(this.tableRef, values);
            return null;
        });
    }

    private Cell cell(int index) {
        return Cell.create(Tables.ROW_BYTES.array(), ("col_" + index).getBytes(StandardCharsets.UTF_8));
    }

}
