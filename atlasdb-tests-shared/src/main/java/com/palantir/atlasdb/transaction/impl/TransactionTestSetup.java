/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.transaction.impl;

import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.TimestampService;
import com.palantir.util.Pair;

public abstract class TransactionTestSetup {
    protected static final TableReference TEST_TABLE = TableReference.createFromFullyQualifiedName(
            "ns.atlasdb_transactions_test_table");

    protected static LockClient lockClient = null;
    protected static LockServiceImpl lockService = null;

    protected static KeyValueService keyValueService;
    protected TimestampService timestampService;
    protected TransactionService transactionService;
    protected ConflictDetectionManager conflictDetectionManager;
    protected SweepStrategyManager sweepStrategyManager;
    protected TransactionManager txMgr;

    @BeforeClass
    public static void setupLockClient() {
        if (lockClient == null) {
            lockClient = LockClient.of("fake lock client");
        }
    }

    @BeforeClass
    public static void setupLockService() {
        if (lockService == null) {
            lockService = LockServiceImpl.create(new LockServerOptions() {
                protected static final long serialVersionUID = 1L;

                @Override
                public boolean isStandaloneServer() {
                    return false;
                }

            });
        }
    }

    @Before
    public void setUp() throws Exception {
        if (keyValueService == null) {
            keyValueService = getKeyValueService();

            keyValueService.createTables(ImmutableMap.of(
                    TEST_TABLE,
                    AtlasDbConstants.GENERIC_TABLE_METADATA,
                    TransactionConstants.TRANSACTION_TABLE,
                    TransactionConstants.TRANSACTION_TABLE_METADATA.persistToBytes()));
            keyValueService.truncateTables(ImmutableSet.of(TEST_TABLE, TransactionConstants.TRANSACTION_TABLE));
        }

        timestampService = new InMemoryTimestampService();

        transactionService = TransactionServices.createTransactionService(keyValueService);
        conflictDetectionManager = ConflictDetectionManagers.createWithoutWarmingCache(keyValueService);
        sweepStrategyManager = SweepStrategyManagers.createDefault(keyValueService);
        txMgr = getManager();
    }

    @After
    public void tearDown() {
        keyValueService.truncateTables(ImmutableSet.of(TEST_TABLE, TransactionConstants.TRANSACTION_TABLE));
    }

    @AfterClass
    public static void tearDownKvs() {
        if (keyValueService != null) {
            keyValueService.close();
            keyValueService = null;
        }
    }

    @AfterClass
    public static void tearDownClass() {
        if (lockService != null) {
            lockService.close();
            lockService = null;
        }
    }

    protected TransactionManager getManager() {
        return new TestTransactionManagerImpl(keyValueService, timestampService, lockClient, lockService,
                transactionService, conflictDetectionManager, sweepStrategyManager);
    }

    protected void put(Transaction txn,
            String rowName,
            String columnName,
            String value) {
        put(txn, TEST_TABLE, rowName, columnName, value);
    }

    protected void put(Transaction txn,
            TableReference tableRef,
            String rowName,
            String columnName,
            String value) {
        Cell cell = Cell.create(PtBytes.toBytes(rowName), PtBytes.toBytes(columnName));
        byte[] valueBytes = value == null ? null : PtBytes.toBytes(value);
        Map<Cell, byte[]> map = Maps.newHashMap();
        map.put(cell, valueBytes);
        txn.put(tableRef, map);
    }

    protected void delete(Transaction txn, String rowName, String columnName) {
        txn.delete(TEST_TABLE, ImmutableSet.of(Cell.create(PtBytes.toBytes(rowName), PtBytes.toBytes(columnName))));
    }

    protected String get(Transaction txn,
            String rowName,
            String columnName) {
        return get(txn, TEST_TABLE, rowName, columnName);
    }

    protected String get(Transaction txn,
            TableReference tableRef,
            String rowName,
            String columnName) {
        byte[] row = PtBytes.toBytes(rowName);
        byte[] column = PtBytes.toBytes(columnName);
        Cell cell = Cell.create(row, column);
        byte[] valueBytes = Cells.convertRowResultsToCells(
                txn.getRows(tableRef,
                        ImmutableSet.of(row),
                        ColumnSelection.create(ImmutableSet.of(column))).values()).get(cell);
        return valueBytes != null ? PtBytes.toString(valueBytes) : null;
    }

    protected String getCell(Transaction txn,
            String rowName,
            String columnName) {
        return getCell(txn, TEST_TABLE, rowName, columnName);
    }

    protected String getCell(Transaction txn,
            TableReference tableRef,
            String rowName,
            String columnName) {
        byte[] row = PtBytes.toBytes(rowName);
        byte[] column = PtBytes.toBytes(columnName);
        Cell cell = Cell.create(row, column);
        Map<Cell, byte[]> map = txn.get(tableRef, ImmutableSet.of(cell));
        byte[] valueBytes = map.get(cell);
        return valueBytes != null ? PtBytes.toString(valueBytes) : null;
    }

    protected Cell getCell(String rowName, String colName) {
        byte[] row = PtBytes.toBytes(rowName);
        return Cell.create(row, PtBytes.toBytes(colName));
    }

    protected void putDirect(String rowName,
            String columnName,
            String value, long timestamp) {
        Cell cell = Cell.create(PtBytes.toBytes(rowName), PtBytes.toBytes(columnName));
        byte[] valueBytes = PtBytes.toBytes(value);
        keyValueService.put(TEST_TABLE, ImmutableMap.of(cell, valueBytes), timestamp);
    }

    protected Pair<String, Long> getDirect(String rowName, String columnName, long timestamp) {
        return getDirect(TEST_TABLE, rowName, columnName, timestamp);
    }

    protected Pair<String, Long> getDirect(TableReference tableRef,
            String rowName,
            String columnName,
            long timestamp) {
        byte[] row = PtBytes.toBytes(rowName);
        Cell cell = Cell.create(row, PtBytes.toBytes(columnName));
        Value valueBytes = keyValueService.get(tableRef, ImmutableMap.of(cell, timestamp)).get(cell);
        return valueBytes != null
                ? Pair.create(PtBytes.toString(valueBytes.getContents()), valueBytes.getTimestamp())
                : null;
    }

    protected abstract KeyValueService getKeyValueService();

}
