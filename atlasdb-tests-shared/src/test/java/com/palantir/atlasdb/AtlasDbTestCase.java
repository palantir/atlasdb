/**
 * Copyright 2015 Palantir Technologies
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

import java.util.Set;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.NamespacedKeyValueServices;
import com.palantir.atlasdb.keyvalue.impl.StatsTrackingKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.TrackingKeyValueService;
import com.palantir.atlasdb.schema.UpgradeSchema;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.impl.CachingTestTransactionManager;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManager;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManagers;
import com.palantir.atlasdb.transaction.impl.SnapshotTransactionManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManagers;
import com.palantir.atlasdb.transaction.impl.TestTransactionManager;
import com.palantir.atlasdb.transaction.impl.TestTransactionManagerImpl;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.TimestampService;

public class AtlasDbTestCase {
    protected static LockClient lockClient;
    protected static LockServiceImpl lockService;

    protected StatsTrackingKeyValueService keyValueServiceWithStats;
    protected TrackingKeyValueService keyValueService;
    protected TimestampService timestampService;
    protected ConflictDetectionManager conflictDetectionManager;
    protected SweepStrategyManager sweepStrategyManager;
    protected TestTransactionManager txManager;
    protected TransactionService transactionService;

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
                private static final long serialVersionUID = 1L;

                @Override
                public boolean isStandaloneServer() {
                    return false;
                }
            });
        }
    }

    @AfterClass
    public static void tearDownLockService() {
        if (lockService != null) {
            lockService.close();
            lockService = null;
        }
    }

    @Before
    public void setUp() throws Exception {
        timestampService = new InMemoryTimestampService();
        KeyValueService kvs = NamespacedKeyValueServices.wrapWithStaticNamespaceMappingKvs(getBaseKeyValueService());
        keyValueServiceWithStats = new StatsTrackingKeyValueService(kvs);
        keyValueService = new TrackingKeyValueService(keyValueServiceWithStats);
        keyValueService.initializeFromFreshInstance();
        SnapshotTransactionManager.createTables(kvs);
        Schemas.createTablesAndIndexes(UpgradeSchema.INSTANCE.getLatestSchema(), kvs);
        transactionService = TransactionServices.createTransactionService(kvs);

        conflictDetectionManager = ConflictDetectionManagers.createDefault(keyValueService);
        sweepStrategyManager = SweepStrategyManagers.createDefault(keyValueService);

        txManager = new TestTransactionManagerImpl(
                keyValueService,
                timestampService,
                lockClient,
                lockService,
                transactionService,
                conflictDetectionManager,
                sweepStrategyManager);
        txManager = new CachingTestTransactionManager(txManager);
    }

    protected KeyValueService getBaseKeyValueService() {
        return new InMemoryKeyValueService(false, PTExecutors.newSingleThreadExecutor(PTExecutors.newNamedThreadFactory(true)));
    }

    @After
    public void tearDown() throws Exception {
        // JUnit keeps instantiated test cases in memory, so we need to null out
        // some fields to prevent OOMs.
        keyValueService.teardown();
        keyValueService = null;
        timestampService = null;
        txManager = null;
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

    protected Set<String> getTablesWrittenTo() {
        return keyValueService.getTablesWrittenTo();
    }

    protected Set<String> getTablesReadFrom() {
        return keyValueService.getTablesReadFrom();
    }

    protected boolean wasTableWrittenTo(String tableName) {
        return getTablesWrittenTo().contains(tableName);
    }

    protected boolean wasTableReadFrom(String tableName) {
        return getTablesReadFrom().contains(tableName);
    }
}
