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
package com.palantir.atlasdb.cli.command;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cleaner.NoOpCleaner;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.TableSplittingKeyValueService;
import com.palantir.atlasdb.schema.KeyValueServiceMigrator;
import com.palantir.atlasdb.schema.KeyValueServiceMigratorUtils;
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.atlasdb.sweep.queue.TargetedSweeper;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManagers;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManagers;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.impl.TransactionTables;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.timelock.paxos.InMemoryTimeLockRule;
import com.palantir.timestamp.ManagedTimestampService;
import java.util.Map;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class KeyValueServiceMigratorsTest {
    private static final long FUTURE_TIMESTAMP = 3141592653589L;
    private static final TableReference TEST_TABLE = TableReference.createFromFullyQualifiedName("test.table");
    private static final TableReference CHECKPOINT_TABLE_NO_NAMESPACE =
            TableReference.createWithEmptyNamespace(KeyValueServiceMigratorUtils.CHECKPOINT_TABLE_NAME);
    private static final TableReference CHECKPOINT_TABLE = TableReference.create(
            KeyValueServiceMigrators.CHECKPOINT_NAMESPACE, KeyValueServiceMigratorUtils.CHECKPOINT_TABLE_NAME);
    private static final ImmutableMap<TableReference, byte[]> TEST_AND_CHECKPOINT_TABLES = ImmutableMap.of(
            TEST_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA,
            CHECKPOINT_TABLE_NO_NAMESPACE, AtlasDbConstants.GENERIC_TABLE_METADATA,
            CHECKPOINT_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
    private static final TableReference FAKE_ATOMIC_TABLE = TableReference.createFromFullyQualifiedName("fake.atomic");
    private static final Cell TEST_CELL = Cell.create(new byte[] {1}, new byte[] {1});
    private static final Cell TEST_CELL2 = Cell.create(new byte[] {2}, new byte[] {2});
    private static final byte[] TEST_VALUE1 = {2};
    private static final byte[] TEST_VALUE2 = {3};

    @Rule
    public InMemoryTimeLockRule fromTimeLock = new InMemoryTimeLockRule("fromClient");

    @Rule
    public InMemoryTimeLockRule toTimeLock = new InMemoryTimeLockRule("toClient");

    private AtlasDbServices fromServices;
    private AtlasDbServices toServices;
    private KeyValueService fromKvs;
    private TransactionManager fromTxManager;
    private KeyValueService toKvs;
    private TransactionManager toTxManager;
    private ImmutableMigratorSpec migratorSpec;

    @Before
    public void setUp() {
        fromServices = createMock(spy(new InMemoryKeyValueService(false)), fromTimeLock);
        toServices = createMock(spy(new InMemoryKeyValueService(false)), toTimeLock);
        fromKvs = fromServices.getKeyValueService();
        fromTxManager = fromServices.getTransactionManager();
        toKvs = toServices.getKeyValueService();
        toTxManager = toServices.getTransactionManager();
        migratorSpec = ImmutableMigratorSpec.builder()
                .fromServices(fromServices)
                .toServices(toServices)
                .build();
    }

    @Test
    public void setupMigratorFastForwardsTimestamp() {
        fromServices.getManagedTimestampService().fastForwardTimestamp(FUTURE_TIMESTAMP);
        assertThat(toServices.getManagedTimestampService().getFreshTimestamp()).isLessThan(FUTURE_TIMESTAMP);

        KeyValueServiceMigrators.setupMigrator(migratorSpec);

        assertThat(toServices.getManagedTimestampService().getFreshTimestamp())
                .isGreaterThanOrEqualTo(FUTURE_TIMESTAMP);
    }

    @Test
    public void setupMigratorCommitsOneTransaction() {
        KeyValueServiceMigrators.setupMigrator(migratorSpec);

        ArgumentCaptor<Long> startTimestampCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<Long> commitTimestampCaptor = ArgumentCaptor.forClass(Long.class);
        verify(toServices.getTransactionService())
                .putUnlessExists(startTimestampCaptor.capture(), commitTimestampCaptor.capture());
        assertThat(startTimestampCaptor.getValue()).isLessThan(commitTimestampCaptor.getValue());
        assertThat(commitTimestampCaptor.getValue())
                .isLessThan(toServices.getManagedTimestampService().getFreshTimestamp());
    }

    @Test
    public void throwsIfSpecifyingNegativeThreads() {
        assertThatThrownBy(() -> ImmutableMigratorSpec.builder()
                        .from(migratorSpec)
                        .threads(-2)
                        .build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void throwsIfSpecifyingNegativeBatchSize() {
        assertThatThrownBy(() -> ImmutableMigratorSpec.builder()
                        .from(migratorSpec)
                        .batchSize(-2)
                        .build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void setupClearsOutExistingTablesExceptAtomic() {
        toKvs.createTables(TEST_AND_CHECKPOINT_TABLES);
        fromKvs.dropTables(fromKvs.getAllTableNames());

        KeyValueServiceMigrator migrator = KeyValueServiceMigrators.setupMigrator(migratorSpec);
        migrator.setup();

        assertThat(toKvs.getAllTableNames())
                .containsExactlyInAnyOrder(
                        TransactionConstants.TRANSACTION_TABLE,
                        TransactionConstants.TRANSACTIONS2_TABLE,
                        AtlasDbConstants.COORDINATION_TABLE);
    }

    @Test
    public void setupCreatesAndResetsExistingTables() {
        fromKvs.createTables(TEST_AND_CHECKPOINT_TABLES);
        toKvs.createTable(TEST_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
        toKvs.put(TEST_TABLE, ImmutableMap.of(TEST_CELL, TEST_VALUE1), 1);
        assertThat(toKvs.get(TEST_TABLE, ImmutableMap.of(TEST_CELL, Long.MAX_VALUE)))
                .isNotEmpty();

        KeyValueServiceMigrator migrator = KeyValueServiceMigrators.setupMigrator(migratorSpec);
        migrator.setup();

        assertThat(toKvs.getAllTableNames()).containsExactlyInAnyOrderElementsOf(fromKvs.getAllTableNames());
        assertThat(toKvs.get(TEST_TABLE, ImmutableMap.of(TEST_CELL, Long.MAX_VALUE)))
                .isEmpty();
    }

    @Test
    public void checkpointTableOnSourceKvsIsIgnored() {
        fromKvs.createTables(TEST_AND_CHECKPOINT_TABLES);

        KeyValueServiceMigrator migrator = KeyValueServiceMigrators.setupMigrator(migratorSpec);
        migrator.setup();
        migrator.migrate();

        verify(fromKvs, never()).get(eq(CHECKPOINT_TABLE), any());
        verify(fromKvs, never()).getRange(eq(CHECKPOINT_TABLE), any(), anyLong());
        verify(fromKvs, never()).getRows(eq(CHECKPOINT_TABLE), any(), any(), anyLong());
        verify(fromKvs, never()).getRowsColumnRange(eq(CHECKPOINT_TABLE), any(), any(), anyLong());
        verify(fromKvs, never()).getRowsColumnRange(eq(CHECKPOINT_TABLE), any(), any(), anyInt(), anyLong());
        verify(fromKvs, never()).getFirstBatchForRanges(eq(CHECKPOINT_TABLE), any(), anyLong());
    }

    @Test
    public void setupCopiesTableMetadata() {
        toKvs.createTables(TEST_AND_CHECKPOINT_TABLES);

        KeyValueServiceMigrator migrator = KeyValueServiceMigrators.setupMigrator(migratorSpec);
        migrator.setup();

        assertThat(fromKvs.getMetadataForTable(TEST_TABLE)).isEqualTo(toKvs.getMetadataForTable(TEST_TABLE));
    }

    @Test
    public void migrateRevertsUncommittedWritesAndMigratesMostRecentlyCommitted() {
        fromKvs.createTables(TEST_AND_CHECKPOINT_TABLES);
        fromTxManager.runTaskWithRetry(tx -> {
            tx.put(TEST_TABLE, ImmutableMap.of(TEST_CELL, TEST_VALUE1));
            return tx.getTimestamp();
        });
        long uncommittedTs = fromServices.getManagedTimestampService().getFreshTimestamp();
        fromKvs.put(TEST_TABLE, ImmutableMap.of(TEST_CELL, TEST_VALUE2), uncommittedTs);

        KeyValueServiceMigrator migrator = KeyValueServiceMigrators.setupMigrator(migratorSpec);
        migrator.setup();
        migrator.migrate();

        assertThat(fromServices.getTransactionService().get(uncommittedTs))
                .isEqualTo(TransactionConstants.FAILED_COMMIT_TS);
        assertThat(toKvs.get(TEST_TABLE, ImmutableMap.of(TEST_CELL, Long.MAX_VALUE))
                        .get(TEST_CELL)
                        .getContents())
                .containsExactly(TEST_VALUE1);
    }

    @Test
    public void migrateOnlyMigratesMostRecentVersions() {
        fromKvs.createTables(TEST_AND_CHECKPOINT_TABLES);
        fromTxManager.runTaskWithRetry(tx -> {
            tx.put(TEST_TABLE, ImmutableMap.of(TEST_CELL, TEST_VALUE1));
            tx.put(TEST_TABLE, ImmutableMap.of(TEST_CELL2, TEST_VALUE1));
            return tx.getTimestamp();
        });
        fromTxManager.runTaskWithRetry(tx -> {
            tx.put(TEST_TABLE, ImmutableMap.of(TEST_CELL, TEST_VALUE2));
            return tx.getTimestamp();
        });

        KeyValueServiceMigrator migrator = KeyValueServiceMigrators.setupMigrator(migratorSpec);
        migrator.setup();
        migrator.migrate();

        toTxManager.runTaskThrowOnConflict(tx -> {
            Map<Cell, byte[]> values = tx.get(TEST_TABLE, ImmutableSet.of(TEST_CELL, TEST_CELL2));
            assertThat(values.get(TEST_CELL)).isEqualTo(TEST_VALUE2);
            assertThat(values.get(TEST_CELL2)).isEqualTo(TEST_VALUE1);
            return null;
        });

        assertThat(toKvs.getAllTimestamps(TEST_TABLE, ImmutableSet.of(TEST_CELL), Long.MAX_VALUE)
                        .size())
                .isEqualTo(1);
    }

    @Test
    public void deletedEntriesAreNotMigrated() {
        fromKvs.createTables(TEST_AND_CHECKPOINT_TABLES);
        fromTxManager.runTaskWithRetry(tx -> {
            tx.put(TEST_TABLE, ImmutableMap.of(TEST_CELL, TEST_VALUE1));
            return tx.getTimestamp();
        });
        fromTxManager.runTaskWithRetry(tx -> {
            tx.delete(TEST_TABLE, ImmutableSet.of(TEST_CELL));
            return tx.getTimestamp();
        });

        KeyValueServiceMigrator migrator = KeyValueServiceMigrators.setupMigrator(migratorSpec);
        migrator.setup();
        migrator.migrate();

        assertThat(toKvs.get(TEST_TABLE, ImmutableMap.of(TEST_CELL, Long.MAX_VALUE)))
                .isEmpty();
    }

    @Test
    public void cleanupDropsCheckpointTable() {
        fromKvs.createTables(TEST_AND_CHECKPOINT_TABLES);
        fromTxManager.runTaskWithRetry(tx -> {
            tx.put(TEST_TABLE, ImmutableMap.of(TEST_CELL, TEST_VALUE1));
            return tx.getTimestamp();
        });

        KeyValueServiceMigrator migrator = KeyValueServiceMigrators.setupMigrator(migratorSpec);
        migrator.setup();
        migrator.migrate();

        verify(toKvs, never()).dropTable(CHECKPOINT_TABLE);

        migrator.cleanup();
        verify(toKvs, times(1)).dropTable(CHECKPOINT_TABLE);
    }

    @Test
    public void tablesDelegatedToSourceKvsGetDroppedFromSourceKvsIfMigratable() {
        fromKvs.createTable(FAKE_ATOMIC_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
        fromKvs.putUnlessExists(FAKE_ATOMIC_TABLE, ImmutableMap.of(TEST_CELL, TEST_VALUE1));

        KeyValueService toTableSplittingKvs = TableSplittingKeyValueService.create(
                ImmutableList.of(new InMemoryKeyValueService(false), fromKvs),
                ImmutableMap.of(FAKE_ATOMIC_TABLE, fromKvs));

        AtlasDbServices toSplittingServices = createMock(toTableSplittingKvs, toTimeLock);
        ImmutableMigratorSpec spec = ImmutableMigratorSpec.builder()
                .fromServices(fromServices)
                .toServices(toSplittingServices)
                .build();

        assertThat(toSplittingServices
                        .getKeyValueService()
                        .get(FAKE_ATOMIC_TABLE, ImmutableMap.of(TEST_CELL, 1L))
                        .get(TEST_CELL)
                        .getContents())
                .containsExactly(TEST_VALUE1);

        KeyValueServiceMigrator migrator = KeyValueServiceMigrators.setupMigrator(spec);
        migrator.setup();

        assertThat(fromKvs.getAllTableNames()).doesNotContain(FAKE_ATOMIC_TABLE);
    }

    @Test
    public void atomicTablesDelegatedToSourceAreNotDropped() {
        KeyValueService toTableSplittingKvs = TableSplittingKeyValueService.create(
                ImmutableList.of(new InMemoryKeyValueService(false), fromKvs),
                Maps.toMap(AtlasDbConstants.HIDDEN_TABLES, _ignore -> fromKvs));

        AtlasDbServices toSplittingServices = createMock(toTableSplittingKvs, toTimeLock);
        ImmutableMigratorSpec spec = ImmutableMigratorSpec.builder()
                .fromServices(fromServices)
                .toServices(toSplittingServices)
                .build();

        fromServices.getTransactionService().putUnlessExists(100_000, 100_001);

        KeyValueServiceMigrator migrator = KeyValueServiceMigrators.setupMigrator(spec);
        migrator.setup();

        assertThat(toSplittingServices.getTransactionService().get(100_000)).isEqualTo(100_001L);
    }

    private static AtlasDbServices createMock(KeyValueService kvs, InMemoryTimeLockRule timeLock) {
        ManagedTimestampService timestampService = timeLock.getManagedTimestampService();

        TransactionTables.createTables(kvs);
        TransactionService transactionService = spy(TransactionServices.createRaw(kvs, timestampService, false));

        AtlasDbServices mockServices = mock(AtlasDbServices.class);
        when(mockServices.getManagedTimestampService()).thenReturn(timestampService);
        when(mockServices.getTransactionService()).thenReturn(transactionService);
        when(mockServices.getKeyValueService()).thenReturn(kvs);
        TargetedSweeper sweeper = TargetedSweeper.createUninitializedForTest(() -> 1);
        SerializableTransactionManager txManager = SerializableTransactionManager.createForTest(
                MetricsManagers.createForTests(),
                kvs,
                timeLock.getLegacyTimelockService(),
                timestampService,
                LockServiceImpl.create(
                        LockServerOptions.builder().isStandaloneServer(false).build()),
                transactionService,
                () -> AtlasDbConstraintCheckingMode.NO_CONSTRAINT_CHECKING,
                ConflictDetectionManagers.createWithoutWarmingCache(kvs),
                SweepStrategyManagers.createDefault(kvs),
                new NoOpCleaner(),
                16,
                4,
                sweeper);
        sweeper.initialize(txManager);
        when(mockServices.getTransactionManager()).thenReturn(txManager);
        return mockServices;
    }
}
