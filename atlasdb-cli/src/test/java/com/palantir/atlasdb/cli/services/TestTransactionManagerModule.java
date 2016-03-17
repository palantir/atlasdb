package com.palantir.atlasdb.cli.services;

import javax.inject.Named;
import javax.inject.Singleton;

import com.google.common.base.Suppliers;
import com.palantir.atlasdb.cleaner.CleanupFollower;
import com.palantir.atlasdb.cleaner.Follower;
import com.palantir.atlasdb.cleaner.NoOpCleaner;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManager;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.lock.LockClient;

import dagger.Module;
import dagger.Provides;

@Module
public class TestTransactionManagerModule {

    @Provides
    @Singleton
    public LockClient provideLockClient() {
        return LockClient.of("atlas instance");
    }

    @Provides
    @Singleton
    public Follower provideCleanupFollower(ServicesConfig atlasDbConfig) {
        return CleanupFollower.create(atlasDbConfig.schemas());
    }

    @Provides
    @Singleton
    public SerializableTransactionManager provideTransactionManager(ServicesConfig config,
                                                                    @Named("kvs") KeyValueService kvs,
                                                                    TransactionManagers.LockAndTimestampServices lts,
                                                                    LockClient lockClient,
                                                                    TransactionService transactionService,
                                                                    ConflictDetectionManager conflictManager,
                                                                    SweepStrategyManager sweepStrategyManager) {
        return new SerializableTransactionManager(
                kvs,
                lts.time(),
                lockClient,
                lts.lock(),
                transactionService,
                Suppliers.ofInstance(AtlasDbConstraintCheckingMode.FULL_CONSTRAINT_CHECKING_THROWS_EXCEPTIONS),
                conflictManager,
                sweepStrategyManager,
                new NoOpCleaner(),
                config.allowAccessToHiddenTables());
    }


}
