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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cleaner.NoOpCleaner;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManagers;
import com.palantir.atlasdb.transaction.impl.SerializableTransaction;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManagers;
import com.palantir.atlasdb.transaction.service.SimpleTransactionService;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampService;

public class KeyValueServiceMigratorsTest {
    private static final long FUTURE_TIMESTAMP = 3141592653589L;
    public static final TableReference TEST_TABLE = TableReference.createFromFullyQualifiedName("test.table");
    public static final TableReference CHECKPOINT_TABLE_NO_NAMESPACE = TableReference.createWithEmptyNamespace(
            com.palantir.atlasdb.schema.KeyValueServiceMigrators.CHECKPOINT_TABLE_NAME);

    private final AtlasDbServices oldServices = createMockAtlasDbServices();
    private final AtlasDbServices newServices = createMockAtlasDbServices();

    private final ImmutableMigratorSpec.Builder migratorSpecBuilder = ImmutableMigratorSpec.builder()
            .fromServices(oldServices)
            .toServices(newServices);

    @Test
    public void setupMigratorFastForwardsTimestamp() {
        KeyValueServiceMigrators.getTimestampManagementService(oldServices).fastForwardTimestamp(FUTURE_TIMESTAMP);
        assertThat(newServices.getTimestampService().getFreshTimestamp()).isLessThan(FUTURE_TIMESTAMP);

        KeyValueServiceMigrators.setupMigrator(migratorSpecBuilder.build());

        assertThat(newServices.getTimestampService().getFreshTimestamp()).isGreaterThanOrEqualTo(FUTURE_TIMESTAMP);
    }

    @Test
    public void setupMigratorCommitsOneTransaction() {
        KeyValueServiceMigrators.setupMigrator(migratorSpecBuilder.build());

        ArgumentCaptor<Long> startTimestampCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<Long> commitTimestampCaptor = ArgumentCaptor.forClass(Long.class);
        verify(newServices.getTransactionService()).putUnlessExists(
                startTimestampCaptor.capture(),
                commitTimestampCaptor.capture());
        assertThat(startTimestampCaptor.getValue()).isLessThan(commitTimestampCaptor.getValue());
        assertThat(commitTimestampCaptor.getValue()).isLessThan(newServices.getTimestampService().getFreshTimestamp());
    }

    @Test
    public void throwsIfSpecifyingNegativeThreads() {
        assertThatThrownBy(() -> migratorSpecBuilder
                .threads(-2)
                .build()).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void throwsIfSpecifyingNegativeBatchSize() {
        assertThatThrownBy(() -> migratorSpecBuilder
                .batchSize(-2)
                .build()).isInstanceOf(IllegalArgumentException.class);
    }

    private static AtlasDbServices createMockAtlasDbServices() {
        TimestampService timestampService = new InMemoryTimestampService();

        AtlasDbServices mockServices = mock(AtlasDbServices.class);
        when(mockServices.getTimestampService()).thenReturn(timestampService);
        when(mockServices.getTransactionService()).thenReturn(mock(TransactionService.class));
        return mockServices;
    }

    @Test
    public void test() {
        AtlasDbServices from = createMock();
        AtlasDbServices to = createMock();

        KeyValueService fromKvs = from.getKeyValueService();
        KeyValueService toKvs = to.getKeyValueService();

        fromKvs.createTables(ImmutableMap.of(TEST_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA,
                CHECKPOINT_TABLE_NO_NAMESPACE, AtlasDbConstants.GENERIC_TABLE_METADATA,
                AtlasDbConstants.hiddenTables.iterator().next(), AtlasDbConstants.GENERIC_TABLE_METADATA));

        System.out.println(fromKvs.getAllTableNames());
        System.out.println(toKvs.getAllTableNames());



    }

    private static AtlasDbServices createMock() {
        KeyValueService kvs = new InMemoryKeyValueService(true);
        TimestampService timestampService = new InMemoryTimestampService();
        TransactionService transactionService = TransactionServices.createTransactionService(kvs);

        AtlasDbServices mockServices = mock(AtlasDbServices.class);
        when(mockServices.getTimestampService()).thenReturn(timestampService);
        when(mockServices.getTransactionService()).thenReturn(transactionService);
        when(mockServices.getKeyValueService()).thenReturn(kvs);
        SerializableTransactionManager txManager = SerializableTransactionManager.createForTest(
                MetricsManagers.createForTests(),
                kvs,
                timestampService,
                (TimestampManagementService) timestampService,
                LockClient.INTERNAL_LOCK_GRANT_CLIENT,
                LockServiceImpl.create(LockServerOptions.builder().isStandaloneServer(false).build()),
                transactionService,
                () -> AtlasDbConstraintCheckingMode.NO_CONSTRAINT_CHECKING,
                ConflictDetectionManagers.createWithoutWarmingCache(kvs),
                SweepStrategyManagers.createDefault(kvs),
                new NoOpCleaner(),
                16,
                4,
                MultiTableSweepQueueWriter.NO_OP);
        when(mockServices.getTransactionManager()).thenReturn(txManager);
        return mockServices;
    }
}
