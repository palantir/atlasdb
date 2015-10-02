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
package com.palantir.atlasdb.shell;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cleaner.NoOpCleaner;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManagers;
import com.palantir.atlasdb.transaction.impl.ShellAwareReadOnlyTransactionManager;
import com.palantir.atlasdb.transaction.impl.SnapshotTransactionManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManagers;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.LockService;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.TimestampService;

public class DefaultAtlasShellContextFactory implements AtlasShellContextFactory {
    private final AtlasDbConstraintCheckingMode atlasdbConstraintCheckingMode = AtlasDbConstraintCheckingMode.FULL_CONSTRAINT_CHECKING_THROWS_EXCEPTIONS;

    @Override
    public AtlasContext withSnapshotTransactionManagerInMemory() {
        KeyValueService keyValueService = new InMemoryKeyValueService(false);
        LockClient lockClient = LockClient.of("in memory atlasdb instance");
        LockService lockService = LockServiceImpl.create(new LockServerOptions() {
            private final static long serialVersionUID = 5836783944180764369L;

            @Override
            public boolean isStandaloneServer() {
                return false;
            }
        });
        TransactionService transactionService = TransactionServices.createTransactionService(keyValueService);
        TimestampService timestampService = new InMemoryTimestampService();
        keyValueService.initializeFromFreshInstance();
        SnapshotTransactionManager.createTables(keyValueService);
        return withSnapshotTransactionManager(
                keyValueService,
                transactionService,
                lockClient,
                lockService,
                timestampService);
    }

    public AtlasContext withShellAwareReadOnlyTransactionManager(
                                                                 KeyValueService keyValueService,
                                                                 TransactionService transactionService) {
        TransactionManager transactionManager = new ShellAwareReadOnlyTransactionManager(
                keyValueService,
                transactionService,
                atlasdbConstraintCheckingMode);
        return getAtlasContext(keyValueService, transactionManager);
    }

    @Override
    public AtlasContext withReadOnlyTransactionManagerCassandra(String host, String port, String keyspace) {
        CassandraKeyValueService kv = CassandraKeyValueService.create(ImmutableCassandraKeyValueServiceConfig.builder()
                .from(CassandraKeyValueServiceConfig.DEFAULT)
                .addServers(host)
                .port(Integer.parseInt(port))
                .poolSize(20)
                .keyspace(keyspace)
                .ssl(false)
                .replicationFactor(1)
                .mutationBatchCount(10000)
                .mutationBatchSizeBytes(10000000)
                .fetchBatchCount(1000)
                .safetyDisabled(false)
                .autoRefreshNodes(false)
                .build());
        TransactionService transactionService = TransactionServices.createTransactionService(kv);
        TransactionManager transactionManager = new ShellAwareReadOnlyTransactionManager(
                kv,
                transactionService,
                atlasdbConstraintCheckingMode);
        return getAtlasContext(kv, transactionManager);
    }

    public AtlasContext withSnapshotTransactionManager(KeyValueService keyValueService,
                                                       TransactionService transactionService,
                                                       LockClient lockClient,
                                                       LockService lockService,
                                                       TimestampService timestampService) {
        Supplier<AtlasDbConstraintCheckingMode> constraintModeSupplier = Suppliers.ofInstance(atlasdbConstraintCheckingMode);
        TransactionManager transactionManager = new SnapshotTransactionManager(
                keyValueService,
                timestampService,
                lockClient,
                lockService,
                transactionService,
                constraintModeSupplier,
                ConflictDetectionManagers.createDefault(keyValueService),
                SweepStrategyManagers.createDefault(keyValueService),
                NoOpCleaner.INSTANCE,
                true);
        return getAtlasContext(keyValueService, transactionManager);
    }

    private AtlasContext getAtlasContext(final KeyValueService keyValueService,
                                         final TransactionManager transactionManager) {
        return new AtlasContext() {
            @Override
            public KeyValueService getKeyValueService() {
                return keyValueService;
            }

            @Override
            public TransactionManager getTransactionManager() {
                return transactionManager;
            }
        };
    }
}
