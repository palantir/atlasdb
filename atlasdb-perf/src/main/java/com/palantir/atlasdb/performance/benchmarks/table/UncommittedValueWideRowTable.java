/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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
import java.util.function.LongFunction;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.performance.backend.AtlasDbServicesConnector;
import com.palantir.atlasdb.performance.benchmarks.Benchmarks;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.util.crypto.Sha256Hash;

/**
 * Table with one wide row, where some proportion of values are uncommitted
 */
public abstract class UncommittedValueWideRowTable {
    private AtlasDbServicesConnector connector;
    private AtlasDbServices services;

    private TableReference tableRef;

    private Map<Cell, Long> allCellsAtMaxTimestamp;
    private Map<Cell, Long> firstCellAtMaxTimestamp;

    public abstract TableReference getTableRef();

    public abstract long numColumnsWithoutUncommittedValues();
    public abstract long numColumnsWithUncommittedValues();
    public abstract long numUncommittedValuesPerColumn();

    public TransactionManager getTransactionManager() {
        return services.getTransactionManager();
    }

    public long getTotalNumColumns() {
        return numColumnsWithoutUncommittedValues()
                + numColumnsWithUncommittedValues() * numUncommittedValuesPerColumn();
    }

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

    private void storeData() {
        services.getTransactionManager().runTaskWithRetry(tx -> {
            putCellsWithoutUncommittedValues(tx);
            putCellsWithUncommittedValues(tx);
            return null;
        });
        for (int i = 0; i < numUncommittedValuesPerColumn(); i++) {
            simulatePuttingValuesInUncommittedTransaction();
        }
    }

    private void simulatePuttingValuesInUncommittedTransaction() {
        long timestamp = services.getTimestampService().getFreshTimestamp();
        Map<Cell, byte[]> cellToBytes = LongStream.range(0, numColumnsWithUncommittedValues())
                .boxed()
                .collect(Collectors.toMap(index -> cell(index, true), PtBytes::toBytes));
        services.getKeyValueService().multiPut(ImmutableMap.of(tableRef, cellToBytes), timestamp);
    }

    private void putCellsWithUncommittedValues(Transaction tx) {
        putCommittedVersionsOfCells(tx, numColumnsWithUncommittedValues(), index -> cell(index, true));
    }

    private void putCellsWithoutUncommittedValues(Transaction tx) {
        putCommittedVersionsOfCells(tx, numColumnsWithoutUncommittedValues(), index -> cell(index, false));
    }

    private void putCommittedVersionsOfCells(Transaction tx, long numColumns, LongFunction<Cell> cellGenerator) {
        Map<Cell, byte[]> x = LongStream.range(0, numColumns)
                .boxed()
                .collect(Collectors.toMap(cellGenerator::apply, PtBytes::toBytes));
        tx.put(tableRef, x);
    }

    @TearDown
    public void cleanup() throws Exception {
        if (!isPersistent()) {
            services.getKeyValueService().dropTables(Sets.newHashSet(tableRef));
        }
        connector.close();
    }

    public abstract boolean isPersistent();

    private Cell cell(long index, boolean hasUncommittedValues) {
        return Cell.create(Tables.ROW_BYTES.array(), getCellName(index, hasUncommittedValues));
    }

    private byte[] getCellName(long index, boolean hasUncommittedValues) {
        String knownComponent = (hasUncommittedValues ? "uc_" : "c_") + index;
        byte[] knownComponentBytes = knownComponent.getBytes(StandardCharsets.UTF_8);
        return EncodingUtils.add(Sha256Hash.computeHash(knownComponentBytes).getBytes(), knownComponentBytes);
    }
}
