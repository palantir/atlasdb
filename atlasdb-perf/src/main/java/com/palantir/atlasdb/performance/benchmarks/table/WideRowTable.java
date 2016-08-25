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

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

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
 * State class for creating a single Atlas table with one wide row.
 */
@State(Scope.Benchmark)
public abstract class WideRowTable {

    public static final TableReference TABLE_REF = TableReference.createFromFullyQualifiedName("performance.table");
    private static final String ROW_COMPONENT = "BIG_ROW_OF_INTS";
    private static final String COLUMN_COMPONENT = "col";
    public static final byte[] ROW_BYTES = ROW_COMPONENT.getBytes(StandardCharsets.UTF_8);

    public static final int NUM_COLS = 50000;

    private AtlasDbServicesConnector connector;
    private AtlasDbServices services;

    private TableReference tableRef;

    private Map<Cell, Long> allCellsAtMaxTimestamp;
    private Map<Cell, Long> firstCellAtMaxTimestamp;

    public TransactionManager getTransactionManager() {
        return services.getTransactionManager();
    }

    public KeyValueService getKvs() {
        return services.getKeyValueService();
    }

    public Map<Cell,Long> getAllCellsAtMaxTimestamp() {
        return allCellsAtMaxTimestamp;
    }

    public Map<Cell,Long> getFirstCellAtMaxTimestampAsMap() {
        return firstCellAtMaxTimestamp;
    }

    public Set<Cell> getAllCells() {
        return allCellsAtMaxTimestamp.keySet();
    }

    public Set<Cell> getFirstCellAsSet() {
        return firstCellAtMaxTimestamp.keySet();
    }

    @Setup
    public void setup(AtlasDbServicesConnector conn) throws UnsupportedEncodingException {
        this.connector = conn;
        services = conn.connect();
        tableRef = Benchmarks.createTableWithDynamicColumns(
                services.getKeyValueService(), TABLE_REF, ROW_COMPONENT, COLUMN_COMPONENT);
        storeData();
    }

    @TearDown
    public void cleanup() throws Exception {
        services.getKeyValueService().dropTables(Sets.newHashSet(tableRef));
        connector.close();
    }

    private void storeData() {
        services.getTransactionManager().runTaskThrowOnConflict(txn -> {
            Map<Cell, byte[]> values = Maps.newHashMap();
            allCellsAtMaxTimestamp = Maps.newHashMap();
            firstCellAtMaxTimestamp = Maps.newHashMap();
            firstCellAtMaxTimestamp.put(cell(0), Long.MAX_VALUE);
            for (int i = 0; i < NUM_COLS; i++) {
                Cell curCell = cell(i);
                values.put(curCell, Ints.toByteArray(i));
                allCellsAtMaxTimestamp.put(curCell, Long.MAX_VALUE) ;
            }
            txn.put(this.tableRef, values);
            return null;
        });
    }

    private Cell cell(int index) {
        return Cell.create(ROW_BYTES, ("col_" + index).getBytes(StandardCharsets.UTF_8));
    }

}
