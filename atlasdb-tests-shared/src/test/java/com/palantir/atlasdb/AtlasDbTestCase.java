/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.StatsTrackingKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.TracingKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.TrackingKeyValueService;
import com.palantir.atlasdb.sweep.queue.KvsSweepQueue;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.impl.CachingTestTransactionManager;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManager;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManagers;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManagers;
import com.palantir.atlasdb.transaction.impl.TestTransactionManager;
import com.palantir.atlasdb.transaction.impl.TestTransactionManagerImpl;
import com.palantir.atlasdb.transaction.impl.TransactionTables;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.atlasdb.util.MetricsRule;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.LockService;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.TimestampService;

public class AtlasDbTestCase {
    protected static LockClient lockClient;
    protected static LockService lockService;

    @Rule
    public MetricsRule metricsRule = new MetricsRule();

    protected StatsTrackingKeyValueService keyValueServiceWithStats;
    protected TrackingKeyValueService keyValueService;
    protected TimestampService timestampService;
    protected ConflictDetectionManager conflictDetectionManager;
    protected SweepStrategyManager sweepStrategyManager;
    protected TestTransactionManagerImpl serializableTxManager;
    protected TestTransactionManager txManager;
    protected TransactionService transactionService;
    protected Map<TableReference, ConflictHandler> conflictHandlerOverrides = new HashMap<>();
    protected MultiTableSweepQueueWriter sweepQueue;

    @BeforeClass
    public static void setupLockClient() {
        if (lockClient == null) {
            lockClient = LockClient.of("fake lock client");
        }
    }

    @BeforeClass
    public static void setupLockService() {
        if (lockService == null) {
            lockService = LockServiceImpl.create(LockServerOptions.builder().isStandaloneServer(false).build());
        }
    }

    @AfterClass
    public static void tearDownLockService() throws IOException {
        if (lockService instanceof Closeable) {
            ((Closeable) lockService).close();
        }
        if (lockService != null) {
            lockService = null;
        }
    }

    @Before
    public void setUp() throws Exception {
        timestampService = new InMemoryTimestampService();
        KeyValueService kvs = getBaseKeyValueService();
        keyValueServiceWithStats = new StatsTrackingKeyValueService(kvs);
        keyValueService = new TrackingKeyValueService(keyValueServiceWithStats);
        TransactionTables.createTables(kvs);
        transactionService = TransactionServices.createTransactionService(kvs);
        conflictDetectionManager = ConflictDetectionManagers.createWithoutWarmingCache(keyValueService);
        sweepStrategyManager = SweepStrategyManagers.createDefault(keyValueService);

        sweepQueue = KvsSweepQueue.createUninitialized(() -> true, () -> 128, 0, 0);

        serializableTxManager = new TestTransactionManagerImpl(
                keyValueService,
                timestampService,
                lockClient,
                lockService,
                transactionService,
                conflictDetectionManager,
                sweepStrategyManager,
                sweepQueue);

        sweepQueue.callbackInit(serializableTxManager);
        txManager = new CachingTestTransactionManager(serializableTxManager);
    }

    protected KeyValueService getBaseKeyValueService() {
        ExecutorService executor = PTExecutors.newSingleThreadExecutor(
                PTExecutors.newNamedThreadFactory(true));
        InMemoryKeyValueService inMemoryKvs = new InMemoryKeyValueService(false, executor);
        KeyValueService tracingKvs = TracingKeyValueService.create(inMemoryKvs);
        return AtlasDbMetrics.instrument(KeyValueService.class, tracingKvs);
    }

    @After
    public void tearDown() throws Exception {
        // JUnit keeps instantiated test cases in memory, so we need to null out
        // some fields to prevent OOMs.
        keyValueService.close();
        keyValueService = null;
        timestampService = null;
        txManager = null;
    }

    protected void overrideConflictHandlerForTable(TableReference table, ConflictHandler conflictHandler) {
        txManager.overrideConflictHandlerForTable(table, conflictHandler);
    }

    protected void setConstraintCheckingMode(AtlasDbConstraintCheckingMode mode) {
        txManager = new TestTransactionManagerImpl(keyValueService,
                timestampService, lockClient, lockService, transactionService, mode);
    }

    protected void clearTablesWrittenTo() {
        keyValueService.clearTablesWrittenTo();
    }

    protected void clearTablesReadFrom() {
        keyValueService.clearTablesReadFrom();
    }

    protected Set<TableReference> getTablesWrittenTo() {
        return keyValueService.getTablesWrittenTo();
    }

    protected Set<TableReference> getTablesReadFrom() {
        return keyValueService.getTablesReadFrom();
    }

    protected boolean wasTableWrittenTo(TableReference tableName) {
        return getTablesWrittenTo().contains(tableName);
    }

    protected boolean wasTableReadFrom(TableReference tableName) {
        return getTablesReadFrom().contains(tableName);
    }
}
