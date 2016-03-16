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
package com.palantir.atlasdb.cli.services;

import java.util.Set;

import javax.inject.Named;
import javax.inject.Singleton;
import javax.net.ssl.SSLSocketFactory;

import com.google.common.base.Optional;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.cleaner.Cleaner;
import com.palantir.atlasdb.cleaner.CleanupFollower;
import com.palantir.atlasdb.cleaner.DefaultCleanerBuilder;
import com.palantir.atlasdb.cleaner.Follower;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.factory.ImmutableLockAndTimestampServices;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.factory.TransactionManagers.LockAndTimestampServices;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.NamespacedKeyValueServices;
import com.palantir.atlasdb.keyvalue.impl.SweepStatsKeyValueService;
import com.palantir.atlasdb.schema.SweepSchema;
import com.palantir.atlasdb.schema.generated.SweepTableFactory;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.sweep.BackgroundSweeper;
import com.palantir.atlasdb.sweep.BackgroundSweeperImpl;
import com.palantir.atlasdb.sweep.SweepTaskRunner;
import com.palantir.atlasdb.sweep.SweepTaskRunnerImpl;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManager;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManagers;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManagers;
import com.palantir.atlasdb.transaction.impl.TransactionTables;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.lock.LockClient;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.client.LockRefreshingRemoteLockService;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.timestamp.TimestampService;

import dagger.Module;
import dagger.Provides;

@Module
public class AtlasDbServicesModule {

    AtlasDbConfig config;

    public AtlasDbServicesModule(AtlasDbConfig config) {
        this.config = config;
    }

    @Provides
    @Singleton
    AtlasDbConfig provideAtlasDbConfig() { return config; }

    @Provides
    @Singleton
    Set<Schema> provideSchemas() {
        return ImmutableSet.of();
    }

    @Provides
    boolean provideAllowAccessToHiddenTables() {
        return true;
    }

    @Provides
    @Singleton
    AtlasDbFactory provideAtlasDbFactory(AtlasDbConfig config) {
        return TransactionManagers.getKeyValueServiceFactory(config.keyValueService().type());
    }

    @Provides
    @Singleton
    @Named("rawKvs")
    KeyValueService provideRawKeyValueService(AtlasDbFactory kvsFactory, AtlasDbConfig config) {
        return kvsFactory.createRawKeyValueService(config.keyValueService());
    }

    @Provides
    public Optional<SSLSocketFactory> provideSslSocketFactory() {
        return Optional.absent();
    }

    @Provides
    @Singleton
    public LockAndTimestampServices provideLockAndTimestampServices(AtlasDbConfig config, @Named("rawKvs") KeyValueService rawKvs, AtlasDbFactory kvsFactory, Optional<SSLSocketFactory> sslSocketFactory) {
        LockAndTimestampServices lts = TransactionManagers.createLockAndTimestampServices(
                config,
                sslSocketFactory,
                resource -> {},
                () -> LockServiceImpl.create(),
                () -> kvsFactory.createTimestampService(rawKvs));
        return ImmutableLockAndTimestampServices.builder()
                .from(lts)
                .lock(LockRefreshingRemoteLockService.create(lts.lock()))
                .build();
    }

    @Provides
    @Singleton
    public TimestampService provideTimestampService(LockAndTimestampServices lts) {
        return lts.time();
    }

    @Provides
    @Singleton
    public RemoteLockService provideLockService(LockAndTimestampServices lts) {
        return lts.lock();
    }

    @Provides
    @Singleton
    @Named("kvs")
    public KeyValueService provideWrappedKeyValueService(@Named("rawKvs") KeyValueService rawKvs, LockAndTimestampServices lts, Set<Schema> schemas) {
        KeyValueService kvs = NamespacedKeyValueServices.wrapWithStaticNamespaceMappingKvs(rawKvs);
        kvs = new SweepStatsKeyValueService(kvs, lts.time());
        TransactionTables.createTables(kvs);

        for (Schema schema : ImmutableSet.<Schema>builder().add(SweepSchema.INSTANCE.getLatestSchema()).addAll(schemas).build()) {
            Schemas.createTablesAndIndexes(schema, kvs);
        }
        return kvs;
    }

    @Provides
    @Singleton
    public LockClient provideLockClient() {
        return LockClient.of("atlas instance");
    }


    @Provides
    @Singleton
    public TransactionService provideTransactionService(@Named("kvs") KeyValueService kvs) {
        return TransactionServices.createTransactionService(kvs);
    }

    @Provides
    @Singleton
    public ConflictDetectionManager provideConflictDetectionManager(@Named("kvs") KeyValueService kvs) {
        return ConflictDetectionManagers.createDefault(kvs);
    }

    @Provides
    @Singleton
    public SweepStrategyManager provideSweepStrategyManager(@Named("kvs") KeyValueService kvs) {
        return SweepStrategyManagers.createDefault(kvs);
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
                                  LockAndTimestampServices lts,
                                  LockClient lockClient,
                                  Follower follower,
                                  TransactionService transactionService) {
        return new DefaultCleanerBuilder(
                kvs,
                lts.lock(),
                lts.time(),
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
                                                                    LockAndTimestampServices lts,
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

    @Provides
    @Singleton
    SweepTaskRunner provideSweepTaskRunner(TransactionManager txm,
                                           @Named("kvs") KeyValueService kvs,
                                           TransactionService transactionService,
                                           SweepStrategyManager sweepStrategyManager,
                                           CleanupFollower follower) {
        return new SweepTaskRunnerImpl(
                txm,
                kvs,
                () -> txm.getUnreadableTimestamp(),
                () -> txm.getImmutableTimestamp(),
                transactionService,
                sweepStrategyManager,
                ImmutableList.<Follower>of(follower));
    }

    @Provides
    @Singleton
    BackgroundSweeper provideBackgroundSweeper(AtlasDbConfig config,
                                               LockAwareTransactionManager txm,
                                               @Named("kvs") KeyValueService kvs,
                                               SweepTaskRunner sweepRunner) {
        BackgroundSweeper backgroundSweeper = new BackgroundSweeperImpl(
                txm,
                kvs,
                sweepRunner,
                Suppliers.ofInstance(config.enableSweep()),
                Suppliers.ofInstance(config.getSweepPauseMillis()),
                Suppliers.ofInstance(config.getSweepBatchSize()),
                SweepTableFactory.of());
        backgroundSweeper.runInBackground();
        return backgroundSweeper;
    }

}
