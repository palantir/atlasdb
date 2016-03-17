package com.palantir.atlasdb.cli.services;

import java.util.Set;

import javax.inject.Named;
import javax.inject.Singleton;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.cleaner.Cleaner;
import com.palantir.atlasdb.cleaner.CleanupFollower;
import com.palantir.atlasdb.cleaner.DefaultCleanerBuilder;
import com.palantir.atlasdb.cleaner.Follower;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManager;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.lock.LockClient;
import com.palantir.lock.RemoteLockService;
import com.palantir.timestamp.TimestampService;

import dagger.Module;
import dagger.Provides;

@Module
public class TransactionManagerModule {

    @Provides
    @Singleton
    public LockClient provideLockClient() {
        return LockClient.of("atlas instance");
    }

    @Provides
    @Singleton
    public Follower provideCleanupFollower(Set<Schema> schemas) {
        return CleanupFollower.create(schemas);
    }

    @Provides
    @Singleton
    public Cleaner provideCleaner(AtlasDbConfig config,
                                  @Named("kvs") KeyValueService kvs,
                                  RemoteLockService rlc,
                                  TimestampService tss,
                                  LockClient lockClient,
                                  Follower follower,
                                  TransactionService transactionService) {
        return new DefaultCleanerBuilder(
                kvs,
                rlc,
                tss,
                lockClient,
                ImmutableList.of(follower),
                transactionService)
                .setBackgroundScrubAggressively(config.backgroundScrubAggressively())
                .setBackgroundScrubBatchSize(config.getBackgroundScrubBatchSize())
                .setBackgroundScrubFrequencyMillis(config.getBackgroundScrubFrequencyMillis())
                .setBackgroundScrubThreads(config.getBackgroundScrubThreads())
                .setPunchIntervalMillis(config.getPunchIntervalMillis())
                .setTransactionReadTimeout(config.getTransactionReadTimeoutMillis())
                .buildCleaner();
    }

    @Provides
    @Singleton
    public SerializableTransactionManager provideTransactionManager(@Named("kvs") KeyValueService kvs,
                                                                    TransactionManagers.LockAndTimestampServices lts,
                                                                    LockClient lockClient,
                                                                    TransactionService transactionService,
                                                                    ConflictDetectionManager conflictManager,
                                                                    SweepStrategyManager sweepStrategyManager,
                                                                    Cleaner cleaner,
                                                                    boolean allowHiddenTableAccess) {
        return new SerializableTransactionManager(
                kvs,
                lts.time(),
                lockClient,
                lts.lock(),
                transactionService,
                Suppliers.ofInstance(AtlasDbConstraintCheckingMode.FULL_CONSTRAINT_CHECKING_THROWS_EXCEPTIONS),
                conflictManager,
                sweepStrategyManager,
                cleaner,
                allowHiddenTableAccess);
    }

}
