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
package com.palantir.atlasdb.cleaner;

import java.io.File;

import org.junit.After;
import org.junit.Before;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.palantir.atlasdb.keyvalue.rocksdb.impl.RocksDbKeyValueService;
import com.palantir.atlasdb.sweep.SweepTaskRunnerImpl;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionManager;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManager;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManagers;
import com.palantir.atlasdb.transaction.impl.SnapshotTransactionManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManagers;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.TimestampService;

public class RocksDbSweeperTest extends AbstractSweeperTest {
    private File tempDir;
    private LockServiceImpl lockService;

    @Before
    @SuppressWarnings("serial")
    public void setup() {
        tempDir = Files.createTempDir();
        kvs = RocksDbKeyValueService.create(tempDir.getAbsolutePath());
        TimestampService tsService = new InMemoryTimestampService();
        LockClient lockClient = LockClient.of("sweep client");
        lockService = LockServiceImpl.create(new LockServerOptions() { @Override public boolean isStandaloneServer() { return false; }});
        txService = TransactionServices.createTransactionService(kvs);
        Supplier<AtlasDbConstraintCheckingMode> constraints = Suppliers.ofInstance(AtlasDbConstraintCheckingMode.NO_CONSTRAINT_CHECKING);
        ConflictDetectionManager cdm = ConflictDetectionManagers.createDefault(kvs);
        SweepStrategyManager ssm = SweepStrategyManagers.createDefault(kvs);
        Cleaner cleaner = new NoOpCleaner();
        LockAwareTransactionManager txManager = new SnapshotTransactionManager(kvs, tsService, lockClient, lockService, txService, constraints, cdm, ssm, cleaner, false);
        SnapshotTransactionManager.createTables(kvs);
        Supplier<Long> tsSupplier = new Supplier<Long>() { @Override public Long get() { return sweepTimestamp.get(); }};
        sweepRunner = new SweepTaskRunnerImpl(txManager, kvs, tsSupplier, tsSupplier, txService, ssm, ImmutableList.<Follower>of());
    }

    @After
    public void teardown() {
        lockService.close();
        kvs.close();
        for (File file : Files.fileTreeTraverser().postOrderTraversal(tempDir)) {
            file.delete();
        }
    }
}
