package com.palantir.atlasdb.cli.api;

import static com.palantir.atlasdb.factory.TransactionManagers.LockAndTimestampServices;

import java.util.Set;

import javax.inject.Named;
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
    AtlasDbConfig provideAtlasDbConfig() { return config; }

    @Provides
    public Set<Schema> provideSchemas() {
        return ImmutableSet.of();
    }

    @Provides
    public boolean provideAllowAccessToHiddenTables() {
        return false;
    }

    @Provides
    public AtlasDbFactory provideAtlasDbFactory(AtlasDbConfig config) {
        return TransactionManagers.getKeyValueServiceFactory(config.keyValueService().type());
    }

    @Provides
    @Named("rawKvs")
    public KeyValueService provideRawKeyValueService(AtlasDbFactory kvsFactory, AtlasDbConfig config) {
        return kvsFactory.createRawKeyValueService(config.keyValueService());
    }

    @Provides
    public Optional<SSLSocketFactory> provideSslSocketFactory() {
        return Optional.absent();
    }

    @Provides
    public LockAndTimestampServices provideLockAndTimestampServices(@Named("rawKvs") KeyValueService rawKvs, AtlasDbFactory kvsFactory, Optional<SSLSocketFactory> sslSocketFactory) {
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
    public TimestampService provideTimestampService(LockAndTimestampServices lts) {
        return lts.time();
    }

    @Provides
    public RemoteLockService provideLockService(LockAndTimestampServices lts) {
        return lts.lock();
    }

    @Provides
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
    public LockClient provideLockClient() {
        return LockClient.of("atlas instance");
    }


    @Provides
    public TransactionService provideTransactionService(@Named("kvs") KeyValueService kvs) {
        return TransactionServices.createTransactionService(kvs);
    }

    @Provides
    public ConflictDetectionManager provideConflictDetectionManager(@Named("kvs") KeyValueService kvs) {
        return ConflictDetectionManagers.createDefault(kvs);
    }

    @Provides
    public SweepStrategyManager provideSweepStrategyManager(@Named("kvs") KeyValueService kvs) {
        return SweepStrategyManagers.createDefault(kvs);
    }

    @Provides
    public Follower provideCleanupFollower(Set<Schema> schemas) {
        return CleanupFollower.create(schemas);
    }

    @Provides
    public Cleaner provideCleaner(@Named("kvs") KeyValueService kvs,
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
    public SweepTaskRunner provideSweepTaskRunner(TransactionManager txm,
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
    BackgroundSweeper provideBackgroundSweeper(LockAwareTransactionManager txm, @Named("kvs") KeyValueService kvs, SweepTaskRunner sweepRunner) {
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
