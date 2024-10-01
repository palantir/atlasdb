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
package com.palantir.atlasdb.services;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.cache.DefaultTimestampCache;
import com.palantir.atlasdb.cell.api.DataKeyValueServiceManager;
import com.palantir.atlasdb.cleaner.CleanupFollower;
import com.palantir.atlasdb.cleaner.DefaultCleanerBuilder;
import com.palantir.atlasdb.cleaner.Follower;
import com.palantir.atlasdb.cleaner.api.Cleaner;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbRuntimeConfig;
import com.palantir.atlasdb.debug.ConflictTracer;
import com.palantir.atlasdb.factory.AtlasDbServiceDiscovery;
import com.palantir.atlasdb.factory.LockAndTimestampServices;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.DelegatingDataKeyValueServiceManager;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.spi.DerivedSnapshotConfig;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.spi.KeyValueServiceRuntimeConfig;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManager;
import com.palantir.atlasdb.transaction.impl.DefaultDeleteExecutor;
import com.palantir.atlasdb.transaction.impl.DefaultOrphanedSentinelDeleter;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.atlasdb.transaction.impl.metrics.DefaultMetricsFilterEvaluationContext;
import com.palantir.atlasdb.transaction.impl.snapshot.DefaultKeyValueSnapshotReaderManager;
import com.palantir.atlasdb.transaction.knowledge.TransactionKnowledgeComponents;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.lock.LockClient;
import com.palantir.lock.v2.TimelockService;
import dagger.Module;
import dagger.Provides;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.util.Optional;
import java.util.concurrent.Executors;
import javax.inject.Named;
import javax.inject.Qualifier;
import javax.inject.Singleton;

@Module
public class TransactionManagerModule {
    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    private @interface Internal {}

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
    public Cleaner provideCleaner(
            ServicesConfig config,
            @Named("kvs") KeyValueService kvs,
            TimelockService timelock,
            Follower follower,
            TransactionService transactionService,
            MetricsManager metricsManager) {
        AtlasDbConfig atlasDbConfig = config.atlasDbConfig();
        return new DefaultCleanerBuilder(kvs, timelock, ImmutableList.of(follower), transactionService, metricsManager)
                .setBackgroundScrubAggressively(atlasDbConfig.backgroundScrubAggressively())
                .setBackgroundScrubBatchSize(atlasDbConfig.getBackgroundScrubBatchSize())
                .setBackgroundScrubFrequencyMillis(atlasDbConfig.getBackgroundScrubFrequencyMillis())
                .setBackgroundScrubThreads(atlasDbConfig.getBackgroundScrubThreads())
                .setPunchIntervalMillis(atlasDbConfig.getPunchIntervalMillis())
                .setTransactionReadTimeout(atlasDbConfig.getTransactionReadTimeoutMillis())
                .setInitializeAsync(atlasDbConfig.initializeAsync())
                .buildCleaner();
    }

    @Provides
    @Singleton
    @Internal
    public DerivedSnapshotConfig provideDerivedSnapshotConfig(
            AtlasDbConfig atlasDbConfig, AtlasDbRuntimeConfig atlasDbRuntimeConfig) {
        KeyValueServiceConfig keyValueServiceConfig = atlasDbConfig.keyValueService();
        Optional<KeyValueServiceRuntimeConfig> keyValueServiceRuntimeConfig = atlasDbRuntimeConfig.keyValueService();
        AtlasDbFactory atlasDbFactory = AtlasDbServiceDiscovery.createAtlasFactoryOfCorrectType(keyValueServiceConfig);
        return atlasDbFactory.createDerivedSnapshotConfig(keyValueServiceConfig, keyValueServiceRuntimeConfig);
    }

    @Provides
    @Singleton
    public SerializableTransactionManager provideTransactionManager(
            MetricsManager metricsManager,
            ServicesConfig config,
            @Named("kvs") KeyValueService kvs,
            LockAndTimestampServices lts,
            TransactionService transactionService,
            ConflictDetectionManager conflictManager,
            SweepStrategyManager sweepStrategyManager,
            Cleaner cleaner,
            @Internal DerivedSnapshotConfig derivedSnapshotConfig,
            TransactionKnowledgeComponents knowledge) {
        DataKeyValueServiceManager dataKeyValueServiceManager = new DelegatingDataKeyValueServiceManager(kvs);
        DefaultDeleteExecutor deleteExecutor = new DefaultDeleteExecutor(
                kvs,
                Executors.newSingleThreadExecutor(
                        new NamedThreadFactory(TransactionManagerModule.class + "-delete-executor", true)));
        // todo(gmaretic): should this be using a real sweep queue?
        return new SerializableTransactionManager(
                metricsManager,
                dataKeyValueServiceManager,
                lts.timelock(),
                lts.lockWatcher(),
                lts.managedTimestampService(),
                lts.lock(),
                transactionService,
                Suppliers.ofInstance(AtlasDbConstraintCheckingMode.FULL_CONSTRAINT_CHECKING_THROWS_EXCEPTIONS),
                conflictManager,
                sweepStrategyManager,
                cleaner,
                new DefaultTimestampCache(metricsManager.getRegistry(), () -> config.atlasDbRuntimeConfig()
                        .getTimestampCacheSize()),
                config.allowAccessToHiddenTables(),
                derivedSnapshotConfig.concurrentGetRangesThreadPoolSize(),
                derivedSnapshotConfig.defaultGetRangesConcurrency(),
                MultiTableSweepQueueWriter.NO_OP,
                deleteExecutor,
                true,
                () -> config.atlasDbRuntimeConfig().transaction(),
                ConflictTracer.NO_OP,
                DefaultMetricsFilterEvaluationContext.createDefault(),
                Optional.empty(),
                knowledge,
                new DefaultKeyValueSnapshotReaderManager(
                        dataKeyValueServiceManager,
                        transactionService,
                        config.allowAccessToHiddenTables(),
                        new DefaultOrphanedSentinelDeleter(sweepStrategyManager::get, deleteExecutor),
                        deleteExecutor));
    }
}
