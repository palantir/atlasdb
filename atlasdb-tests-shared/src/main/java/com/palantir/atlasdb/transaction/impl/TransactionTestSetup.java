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
package com.palantir.atlasdb.transaction.impl;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.ComparingTimestampCache;
import com.palantir.atlasdb.cache.TimestampCache;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.api.watch.LockWatchManager;
import com.palantir.atlasdb.keyvalue.api.watch.NoOpLockWatchManager;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.keyvalue.impl.KvsManager;
import com.palantir.atlasdb.keyvalue.impl.TransactionManagerManager;
import com.palantir.atlasdb.persistent.api.PersistentStore;
import com.palantir.atlasdb.persistent.rocksdb.RocksDbPersistentStore;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.table.description.TableMetadata;
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
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampService;
import com.palantir.util.Pair;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public abstract class TransactionTestSetup {
    @ClassRule
    public static final TemporaryFolder PERSISTENT_STORAGE_FOLDER = new TemporaryFolder();
    private static PersistentStore persistentStore;

    @BeforeClass
    public static void storageSetUp() throws IOException, RocksDBException {
        File storageDirectory = PERSISTENT_STORAGE_FOLDER.newFolder();
        RocksDB rocksDb = RocksDB.open(storageDirectory.getAbsolutePath());
        persistentStore = new RocksDbPersistentStore(rocksDb, storageDirectory);
    }

    @AfterClass
    public static void storageTearDown() throws Exception {
        persistentStore.close();
    }

    protected static final TableReference TEST_TABLE = TableReference.createFromFullyQualifiedName(
            "ns.atlasdb_transactions_test_table");
    protected static final TableReference TEST_TABLE_THOROUGH = TableReference.createFromFullyQualifiedName(
            "ns.atlasdb_transactions_test_table_thorough");

    private final KvsManager kvsManager;
    private final TransactionManagerManager tmManager;

    protected LockClient lockClient;
    protected LockServiceImpl lockService;
    protected TimelockService timelockService;
    protected LockWatchManager lockWatchManager;

    protected final MetricsManager metricsManager = MetricsManagers.createForTests();
    protected KeyValueService keyValueService;
    protected TimestampService timestampService;
    protected TimestampManagementService timestampManagementService;
    protected TransactionService transactionService;
    protected ConflictDetectionManager conflictDetectionManager;
    protected SweepStrategyManager sweepStrategyManager;
    protected TransactionManager txMgr;

    protected TimestampCache timestampCache;

    protected TransactionTestSetup(KvsManager kvsManager, TransactionManagerManager tmManager) {
        this.kvsManager = kvsManager;
        this.tmManager = tmManager;
    }

    @Before
    public void setUp() {
        timestampCache = ComparingTimestampCache.comparingOffHeapForTests(metricsManager, persistentStore);

        lockService = LockServiceImpl.create(LockServerOptions.builder().isStandaloneServer(false).build());
        lockClient = LockClient.of("test_client");

        keyValueService = getKeyValueService();
        keyValueService.createTables(ImmutableMap.of(
                TEST_TABLE_THOROUGH,
                TableMetadata.builder()
                        .rangeScanAllowed(true)
                        .explicitCompressionBlockSizeKB(4)
                        .negativeLookups(true)
                        .sweepStrategy(SweepStrategy.THOROUGH)
                        .build()
                        .persistToBytes(),
                TEST_TABLE,
                TableMetadata.builder()
                        .rangeScanAllowed(true)
                        .explicitCompressionBlockSizeKB(4)
                        .negativeLookups(true)
                        .sweepStrategy(SweepStrategy.NOTHING)
                        .build()
                        .persistToBytes()));
        TransactionTables.createTables(keyValueService);
        TransactionTables.truncateTables(keyValueService);
        keyValueService.truncateTable(TEST_TABLE);

        InMemoryTimestampService ts = new InMemoryTimestampService();
        timestampService = ts;
        timestampManagementService = ts;
        timelockService = new LegacyTimelockService(timestampService, lockService, lockClient);
        lockWatchManager = NoOpLockWatchManager.INSTANCE;
        transactionService = TransactionServices.createRaw(keyValueService, timestampService, false);
        conflictDetectionManager = ConflictDetectionManagers.createWithoutWarmingCache(keyValueService);
        sweepStrategyManager = SweepStrategyManagers.createDefault(keyValueService);
        txMgr = getManager();
    }

    protected KeyValueService getKeyValueService() {
        return kvsManager.getDefaultKvs();
    }

    protected TransactionManager getManager() {
        return tmManager.getLastRegisteredTransactionManager().orElseGet(this::createAndRegisterManager);
    }

    TransactionManager createAndRegisterManager() {
        TransactionManager manager = createManager();
        tmManager.registerTransactionManager(manager);
        return manager;
    }

    protected TransactionManager createManager() {
        return new TestTransactionManagerImpl(
                MetricsManagers.createForTests(),
                keyValueService, timestampService, timestampManagementService, lockClient, lockService,
                transactionService, conflictDetectionManager, sweepStrategyManager, timestampCache,
                MultiTableSweepQueueWriter.NO_OP,
                MoreExecutors.newDirectExecutorService());
    }

    protected void put(Transaction txn, String rowName, String columnName, String value) {
        put(txn, TEST_TABLE, rowName, columnName, value);
    }

    protected void put(Transaction txn, TableReference tableRef, String rowName, String columnName, String value) {
        Cell cell = createCell(rowName, columnName);
        byte[] valueBytes = value == null ? null : PtBytes.toBytes(value);
        Map<Cell, byte[]> map = Maps.newHashMap();
        map.put(cell, valueBytes);
        txn.put(tableRef, map);
    }

    protected void delete(Transaction txn, String rowName, String columnName) {
        txn.delete(TEST_TABLE, ImmutableSet.of(createCell(rowName, columnName)));
    }

    protected String get(Transaction txn, String rowName, String columnName) {
        return get(txn, TEST_TABLE, rowName, columnName);
    }

    protected String get(Transaction txn, TableReference tableRef, String rowName, String columnName) {
        byte[] row = PtBytes.toBytes(rowName);
        byte[] column = PtBytes.toBytes(columnName);
        Cell cell = Cell.create(row, column);
        byte[] valueBytes = Cells.convertRowResultsToCells(
                txn.getRows(tableRef,
                        ImmutableSet.of(row),
                        ColumnSelection.create(ImmutableSet.of(column))).values()).get(cell);
        return valueBytes != null ? PtBytes.toString(valueBytes) : null;
    }

    String getCell(Transaction txn, String rowName, String columnName) {
        return getCell(txn, TEST_TABLE, rowName, columnName);
    }

    private String getCell(Transaction txn, TableReference tableRef, String rowName, String columnName) {
        Cell cell = createCell(rowName, columnName);
        Map<Cell, byte[]> map = txn.get(tableRef, ImmutableSet.of(cell));
        byte[] valueBytes = map.get(cell);
        return valueBytes != null ? PtBytes.toString(valueBytes) : null;
    }

    void putDirect(String rowName, String columnName, String value, long timestamp) {
        Cell cell = createCell(rowName, columnName);
        byte[] valueBytes = PtBytes.toBytes(value);
        keyValueService.put(TEST_TABLE, ImmutableMap.of(cell, valueBytes), timestamp);
    }

    Pair<String, Long> getDirect(String rowName, String columnName, long timestamp) {
        return getDirect(TEST_TABLE, rowName, columnName, timestamp);
    }

    Pair<String, Long> getDirect(TableReference tableRef, String rowName, String columnName, long timestamp) {
        Cell cell = createCell(rowName, columnName);
        Value valueBytes = keyValueService.get(tableRef, ImmutableMap.of(cell, timestamp)).get(cell);
        return valueBytes != null
                ? Pair.create(PtBytes.toString(valueBytes.getContents()), valueBytes.getTimestamp())
                : null;
    }

    Cell createCell(String rowName, String columnName) {
        return Cell.create(PtBytes.toBytes(rowName), PtBytes.toBytes(columnName));
    }
}
