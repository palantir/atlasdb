package com.palantir.atlasdb.shell;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.palantir.atlasdb.cleaner.NoOpCleaner;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
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

public class InMemoryAtlasShellContextFactory implements AtlasShellContextFactory {
    private final AtlasDbConstraintCheckingMode atlasdbConstraintCheckingMode = AtlasDbConstraintCheckingMode.FULL_CONSTRAINT_CHECKING_THROWS_EXCEPTIONS;

    @Override
    public AtlasContext withShellAwareReadOnlyTransactionManagerFromDb(String host,
                                                                       String port,
                                                                       String sid,
                                                                       String type,
                                                                       String username,
                                                                       String password) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AtlasContext withSnapshotTransactionManagerFromDispatchPrefs() {
        throw new UnsupportedOperationException();
    }

    @Override
    public AtlasContext withSnapshotTransactionManagerFromDispatch(String host,
                                                                   String port,
                                                                   String user,
                                                                   String pass) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AtlasContext withShellAwareReadOnlyTransactionManagerInMemory() {
        KeyValueService keyValueService = new InMemoryKeyValueService(false);
        TransactionService transactionService = TransactionServices.createTransactionService(keyValueService);
        keyValueService.initializeFromFreshInstance();
        SnapshotTransactionManager.createTables(keyValueService);
        AtlasContext atlasContext = withShellAwareReadOnlyTransactionManager(
                keyValueService,
                transactionService);
        return atlasContext;
    }

    @Override
    public AtlasContext withSnapshotTransactionManagerInMemory() {
        KeyValueService keyValueService = new InMemoryKeyValueService(false);
        LockClient lockClient = LockClient.of("in memory atlas instance");
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

    @Override
    public AtlasContext withShellAwareReadOnlyTransactionManager(KeyValueService keyValueService,
                                                                 TransactionService transactionService) {
        TransactionManager transactionManager = new ShellAwareReadOnlyTransactionManager(
                keyValueService,
                transactionService,
                atlasdbConstraintCheckingMode);
        return getAtlasContext(keyValueService, transactionManager);
    }

    @Override
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
