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
import org.junit.Before;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cache.TimestampCache;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.impl.LegacyTimelockService;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.lock.v2.TimelockService;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.TimestampService;
import com.palantir.util.Pair;

public abstract class TransactionTestSetup {
    protected static final TableReference TEST_TABLE = TableReference.createFromFullyQualifiedName(
            "ns.atlasdb_transactions_test_table");
    protected static final TableReference TEST_TABLE_THOROUGH = TableReference.createFromFullyQualifiedName(
            "ns.atlasdb_transactions_test_table_thorough");

    protected LockClient lockClient;
    protected LockServiceImpl lockService;
    protected TimelockService timelockService;

    protected final MetricsManager metricsManager = MetricsManagers.createForTests();
    protected KeyValueService keyValueService;
    protected TimestampService timestampService;
    protected TransactionService transactionService;
    protected ConflictDetectionManager conflictDetectionManager;
    protected SweepStrategyManager sweepStrategyManager;
    protected TransactionManager txMgr;

    protected final TimestampCache timestampCache = new TimestampCache(
            new MetricRegistry(),
            () -> AtlasDbConstants.DEFAULT_TIMESTAMP_CACHE_SIZE);

    @Before
    public void setUp() throws Exception {
        lockService = LockServiceImpl.create(LockServerOptions.builder().isStandaloneServer(false).build());
        lockClient = LockClient.of("test_client");

        keyValueService = getKeyValueService();
        keyValueService.createTables(ImmutableMap.of(
                TEST_TABLE_THOROUGH,
                new TableMetadata(
                        new NameMetadataDescription(),
                        new ColumnMetadataDescription(),
                        ConflictHandler.RETRY_ON_WRITE_WRITE,
                        TableMetadataPersistence.CachePriority.WARM,
                        true,
                        4,
                        true,
                        TableMetadataPersistence.SweepStrategy.THOROUGH,
                        false,
                        TableMetadataPersistence.LogSafety.UNSAFE).persistToBytes(),
                TEST_TABLE,
                new TableMetadata(
                        new NameMetadataDescription(),
                        new ColumnMetadataDescription(),
                        ConflictHandler.RETRY_ON_WRITE_WRITE,
                        TableMetadataPersistence.CachePriority.WARM,
                        true,
                        4,
                        true,
                        TableMetadataPersistence.SweepStrategy.NOTHING,
                        false,
                        TableMetadataPersistence.LogSafety.UNSAFE).persistToBytes(),
                TransactionConstants.TRANSACTION_TABLE,
                TransactionConstants.TRANSACTION_TABLE_METADATA.persistToBytes()));
        keyValueService.truncateTables(ImmutableSet.of(TEST_TABLE, TransactionConstants.TRANSACTION_TABLE));

        timestampService = new InMemoryTimestampService();
        timelockService = new LegacyTimelockService(timestampService, lockService, lockClient);

        transactionService = TransactionServices.createTransactionService(keyValueService);
        conflictDetectionManager = ConflictDetectionManagers.createWithoutWarmingCache(keyValueService);
        sweepStrategyManager = SweepStrategyManagers.createDefault(keyValueService);
        txMgr = getManager();
    }

    @After
    public void tearDown() {
        lockService.close();
        keyValueService.close();
    }

    protected TransactionManager getManager() {
        return new TestTransactionManagerImpl(
                MetricsManagers.createForTests(),
                keyValueService, timestampService, lockClient, lockService,
                transactionService, conflictDetectionManager, sweepStrategyManager, MultiTableSweepQueueWriter.NO_OP,
                MoreExecutors.newDirectExecutorService());
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
