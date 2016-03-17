package com.palantir.atlasdb.cli.services;

import javax.inject.Named;
import javax.inject.Singleton;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.cleaner.Follower;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.schema.generated.SweepTableFactory;
import com.palantir.atlasdb.sweep.BackgroundSweeper;
import com.palantir.atlasdb.sweep.BackgroundSweeperImpl;
import com.palantir.atlasdb.sweep.SweepTaskRunner;
import com.palantir.atlasdb.sweep.SweepTaskRunnerImpl;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.atlasdb.transaction.service.TransactionService;

import dagger.Module;
import dagger.Provides;

@Module
public class SweeperModule {

    @Provides
    @Singleton
    public SweepTaskRunner provideSweepTaskRunner(SerializableTransactionManager txm,
                                                  @Named("kvs") KeyValueService kvs,
                                                  TransactionService transactionService,
                                                  SweepStrategyManager sweepStrategyManager,
                                                  Follower follower) {
        return new SweepTaskRunnerImpl(
                txm,
                kvs,
                txm::getUnreadableTimestamp,
                txm::getImmutableTimestamp,
                transactionService,
                sweepStrategyManager,
                ImmutableList.of(follower));
    }

}
