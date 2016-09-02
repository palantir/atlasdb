/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.services;

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
    public Follower provideCleanupFollower(ServicesConfig atlasDbConfig) {
        return CleanupFollower.create(atlasDbConfig.schemas());
    }

    @Provides
    @Singleton
    public Cleaner provideCleaner(ServicesConfig config,
                                  @Named("kvs") KeyValueService kvs,
                                  RemoteLockService rlc,
                                  TimestampService tss,
                                  LockClient lockClient,
                                  Follower follower,
                                  TransactionService transactionService) {
        AtlasDbConfig atlasDbConfig = config.atlasDbConfig();
        return new DefaultCleanerBuilder(
                kvs,
                rlc,
                tss,
                lockClient,
                ImmutableList.of(follower),
                transactionService)
                .setBackgroundScrubAggressively(atlasDbConfig.backgroundScrubAggressively())
                .setBackgroundScrubBatchSize(atlasDbConfig.getBackgroundScrubBatchSize())
                .setBackgroundScrubFrequencyMillis(atlasDbConfig.getBackgroundScrubFrequencyMillis())
                .setBackgroundScrubThreads(atlasDbConfig.getBackgroundScrubThreads())
                .setPunchIntervalMillis(atlasDbConfig.getPunchIntervalMillis())
                .setTransactionReadTimeout(atlasDbConfig.getTransactionReadTimeoutMillis())
                .buildCleaner();
    }

    @Provides
    @Singleton
    public SerializableTransactionManager provideTransactionManager(ServicesConfig config,
                                                                    @Named("kvs") KeyValueService kvs,
                                                                    TransactionManagers.LockAndTimestampServices lts,
                                                                    LockClient lockClient,
                                                                    TransactionService transactionService,
                                                                    ConflictDetectionManager conflictManager,
                                                                    SweepStrategyManager sweepStrategyManager,
                                                                    Cleaner cleaner) {
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
                config.allowAccessToHiddenTables());
    }

}
