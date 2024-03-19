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

import static com.palantir.atlasdb.transaction.service.TransactionServices.createTransactionService;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.ComparingTimestampCache;
import com.palantir.atlasdb.cache.TimestampCache;
import com.palantir.atlasdb.cell.api.TransactionKeyValueServiceManager;
import com.palantir.atlasdb.coordination.CoordinationService;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.internalschema.ImmutableInternalSchemaInstallConfig;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadata;
import com.palantir.atlasdb.internalschema.TransactionSchemaManager;
import com.palantir.atlasdb.internalschema.persistence.CoordinationServices;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.api.watch.LockWatchManagerInternal;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.keyvalue.impl.DelegatingTransactionKeyValueServiceManager;
import com.palantir.atlasdb.keyvalue.impl.KvsManager;
import com.palantir.atlasdb.keyvalue.impl.TransactionManagerManager;
import com.palantir.atlasdb.persistent.api.PersistentStore;
import com.palantir.atlasdb.persistent.rocksdb.RocksDbPersistentStore;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.transaction.ImmutableTransactionConfig;
import com.palantir.atlasdb.transaction.TransactionConfig;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.DeleteExecutor;
import com.palantir.atlasdb.transaction.api.KeyValueSnapshotReaderManager;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.knowledge.TransactionKnowledgeComponents;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.LockService;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.lock.v2.TimelockService;
import com.palantir.test.utils.SubdirectoryCreator;
import com.palantir.timelock.paxos.InMemoryTimelockExtension;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampService;
import com.palantir.util.Pair;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/**
 * Expectations for semantics of tests that are run as part of this class (not that the author thinks these
 * assumptions are ideal, but this is documentation of the current state):
 * - The timestamp and lock services are disjoint across individual tests.
 * - The key value services may not be disjoint across individual tests.
 */
public abstract class TransactionTestSetup {
    @TempDir
    public static File persistentStorageFolder;

    private static PersistentStore persistentStore;

    @BeforeAll
    public static void storageSetUp() throws RocksDBException {
        File storageDirectory =
                SubdirectoryCreator.createAndGetSubdirectory(persistentStorageFolder, "TransactionTestSetupV2");
        RocksDB rocksDb = RocksDB.open(storageDirectory.getAbsolutePath());
        persistentStore = new RocksDbPersistentStore(rocksDb, storageDirectory);
    }

    @AfterAll
    public static void storageTearDown() throws Exception {
        persistentStore.close();
    }

    protected static final TableReference TEST_TABLE =
            TableReference.createFromFullyQualifiedName("ns.atlasdb_transactions_test_table");
    protected static final TableReference TEST_TABLE_THOROUGH =
            TableReference.createFromFullyQualifiedName("ns.atlasdb_transactions_test_table_thorough");
    protected static final TableReference TEST_TABLE_SERIALIZABLE =
            TableReference.createFromFullyQualifiedName("ns.atlasdb_transactions_test_table_serializable");

    private final KvsManager kvsManager;
    private final TransactionManagerManager tmManager;

    protected LockClient lockClient;
    protected LockService lockService;
    protected TimelockService timelockService;
    protected LockWatchManagerInternal lockWatchManager;

    protected final MetricsManager metricsManager = MetricsManagers.createForTests();
    protected KeyValueService keyValueService;
    protected TransactionKeyValueServiceManager transactionKeyValueServiceManager;
    protected TimestampService timestampService;
    protected TimestampManagementService timestampManagementService;
    protected TransactionSchemaManager transactionSchemaManager;
    protected TransactionService transactionService;
    protected ConflictDetectionManager conflictDetectionManager;
    protected SweepStrategyManager sweepStrategyManager;
    protected TransactionManager txMgr;
    protected DeleteExecutor deleteExecutor;

    protected TimestampCache timestampCache;

    protected TransactionKnowledgeComponents knowledge;
    protected Supplier<TransactionConfig> transactionConfigSupplier;
    protected CommitTimestampLoaderFactory commitTimestampLoaderFactory;
    protected KeyValueSnapshotReaderManager keyValueSnapshotReaderManager;

    @RegisterExtension
    public InMemoryTimelockExtension inMemoryTimelockExtension = new InMemoryTimelockExtension();

    protected TransactionTestSetup(KvsManager kvsManager, TransactionManagerManager tmManager) {
        this.kvsManager = kvsManager;
        this.tmManager = tmManager;
    }

    @BeforeEach
    public void setUp() {
        timestampCache = ComparingTimestampCache.comparingOffHeapForTests(metricsManager, persistentStore);

        lockService = LockServiceImpl.create(
                LockServerOptions.builder().isStandaloneServer(false).build());
        lockClient = LockClient.of("test_client");

        keyValueService = getKeyValueService();
        transactionKeyValueServiceManager = new DelegatingTransactionKeyValueServiceManager(keyValueService);
        deleteExecutor = new DefaultDeleteExecutor(
                transactionKeyValueServiceManager.getKeyValueService().orElseThrow(),
                MoreExecutors.newDirectExecutorService());
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
                        .persistToBytes(),
                TEST_TABLE_SERIALIZABLE,
                TableMetadata.builder()
                        .rangeScanAllowed(true)
                        .explicitCompressionBlockSizeKB(4)
                        .negativeLookups(true)
                        .conflictHandler(ConflictHandler.SERIALIZABLE)
                        .sweepStrategy(SweepStrategy.NOTHING)
                        .build()
                        .persistToBytes()));
        TransactionTables.createTables(keyValueService);
        TransactionTables.truncateTables(keyValueService);
        keyValueService.truncateTable(TEST_TABLE);
        keyValueService.truncateTable(TEST_TABLE_SERIALIZABLE);

        timestampService = inMemoryTimelockExtension.getTimestampService();
        timestampManagementService = inMemoryTimelockExtension.getTimestampManagementService();
        timelockService = inMemoryTimelockExtension.getLegacyTimelockService();
        lockWatchManager = inMemoryTimelockExtension.getLockWatchManager();

        CoordinationService<InternalSchemaMetadata> coordinationService =
                CoordinationServices.createDefault(keyValueService, timestampService, metricsManager, false);
        transactionSchemaManager = new TransactionSchemaManager(coordinationService);
        knowledge = TransactionKnowledgeComponents.create(
                keyValueService,
                metricsManager.getTaggedRegistry(),
                ImmutableInternalSchemaInstallConfig.builder().build(),
                () -> true);
        transactionService = createTransactionService(keyValueService, transactionSchemaManager, knowledge);
        transactionConfigSupplier = () -> ImmutableTransactionConfig.builder().build();
        commitTimestampLoaderFactory = new CommitTimestampLoaderFactory(
                timestampCache,
                metricsManager,
                timelockService,
                knowledge,
                transactionService,
                transactionConfigSupplier);
        conflictDetectionManager = ConflictDetectionManagers.createWithoutWarmingCache(keyValueService);
        sweepStrategyManager = SweepStrategyManagers.createDefault(keyValueService);
        keyValueSnapshotReaderManager = new DefaultKeyValueSnapshotReaderManager(
                transactionKeyValueServiceManager,
                transactionService,
                false,
                new DefaultOrphanedSentinelDeleter(sweepStrategyManager::get, deleteExecutor),
                deleteExecutor);
        txMgr = createAndRegisterManager();
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
                keyValueService,
                inMemoryTimelockExtension,
                lockService,
                transactionService,
                conflictDetectionManager,
                sweepStrategyManager,
                timestampCache,
                MultiTableSweepQueueWriter.NO_OP,
                knowledge,
                MoreExecutors.newDirectExecutorService(),
                keyValueSnapshotReaderManager);
    }

    protected void put(Transaction txn, String rowName, String columnName, String value) {
        put(txn, TEST_TABLE, rowName, columnName, value);
    }

    protected void put(Transaction txn, byte[] rowName, byte[] columnName, String value) {
        put(txn, TEST_TABLE, rowName, columnName, value);
    }

    protected void put(Transaction txn, TableReference tableRef, String rowName, String columnName, String value) {
        put(txn, tableRef, PtBytes.toBytes(rowName), PtBytes.toBytes(columnName), value);
    }

    protected void put(Transaction txn, TableReference tableRef, byte[] rowName, byte[] columnName, String value) {
        Cell cell = createCell(rowName, columnName);
        byte[] valueBytes = value == null ? null : PtBytes.toBytes(value);
        Map<Cell, byte[]> map = new HashMap<>();
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
                        txn.getRows(tableRef, ImmutableSet.of(row), ColumnSelection.create(ImmutableSet.of(column)))
                                .values())
                .get(cell);
        return valueBytes != null ? PtBytes.toString(valueBytes) : null;
    }

    public String getCell(Transaction txn, TableReference tableRef, String rowName, String columnName) {
        Cell cell = createCell(rowName, columnName);
        Map<Cell, byte[]> map = txn.get(tableRef, ImmutableSet.of(cell));
        byte[] valueBytes = map.get(cell);
        return valueBytes != null ? PtBytes.toString(valueBytes) : null;
    }

    protected void putDirect(String rowName, String columnName, String value, long timestamp) {
        Cell cell = createCell(rowName, columnName);
        byte[] valueBytes = PtBytes.toBytes(value);
        keyValueService.put(TEST_TABLE, ImmutableMap.of(cell, valueBytes), timestamp);
    }

    Pair<String, Long> getDirect(String rowName, String columnName, long timestamp) {
        return getDirect(TEST_TABLE, rowName, columnName, timestamp);
    }

    Pair<String, Long> getDirect(TableReference tableRef, String rowName, String columnName, long timestamp) {
        Cell cell = createCell(rowName, columnName);
        Value valueBytes =
                keyValueService.get(tableRef, ImmutableMap.of(cell, timestamp)).get(cell);
        return valueBytes != null
                ? Pair.create(PtBytes.toString(valueBytes.getContents()), valueBytes.getTimestamp())
                : null;
    }

    Cell createCell(String rowName, String columnName) {
        return Cell.create(PtBytes.toBytes(rowName), PtBytes.toBytes(columnName));
    }

    Cell createCell(byte[] rowName, byte[] columnName) {
        return Cell.create(rowName, columnName);
    }
}
