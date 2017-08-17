/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.factory;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ForwardingObject;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.atlasdb.cleaner.Cleaner;
import com.palantir.atlasdb.cleaner.CleanupFollower;
import com.palantir.atlasdb.cleaner.DefaultCleanerBuilder;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbRuntimeConfig;
import com.palantir.atlasdb.config.ImmutableTimestampClientConfig;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.config.SweepConfig;
import com.palantir.atlasdb.config.TimeLockClientConfig;
import com.palantir.atlasdb.config.TimestampClientConfig;
import com.palantir.atlasdb.factory.TransactionManagers.LockAndTimestampServices;
import com.palantir.atlasdb.factory.startup.TimeLockMigrator;
import com.palantir.atlasdb.factory.timestamp.DecoratedTimelockServices;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.NamespacedKeyValueServices;
import com.palantir.atlasdb.keyvalue.impl.ProfilingKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.SweepStatsKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.TracingKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.ValidatingQueryRewritingKeyValueService;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.persistentlock.CheckAndSetExceptionMapper;
import com.palantir.atlasdb.persistentlock.InitialisingPersistentLockService;
import com.palantir.atlasdb.persistentlock.KvsBackedPersistentLockService;
import com.palantir.atlasdb.persistentlock.NoOpPersistentLockService;
import com.palantir.atlasdb.persistentlock.PersistentLockService;
import com.palantir.atlasdb.schema.SweepSchema;
import com.palantir.atlasdb.schema.generated.SweepTableFactory;
import com.palantir.atlasdb.sweep.BackgroundSweeperImpl;
import com.palantir.atlasdb.sweep.BackgroundSweeperPerformanceLogger;
import com.palantir.atlasdb.sweep.CellsSweeper;
import com.palantir.atlasdb.sweep.ImmutableSweepBatchConfig;
import com.palantir.atlasdb.sweep.InitialisingSweeperService;
import com.palantir.atlasdb.sweep.NoOpBackgroundSweeperPerformanceLogger;
import com.palantir.atlasdb.sweep.PersistentLockManager;
import com.palantir.atlasdb.sweep.SpecificTableSweeper;
import com.palantir.atlasdb.sweep.SweepBatchConfig;
import com.palantir.atlasdb.sweep.SweepMetrics;
import com.palantir.atlasdb.sweep.SweepTaskRunner;
import com.palantir.atlasdb.sweep.SweeperServiceImpl;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.KeyValueServiceStatus;
import com.palantir.atlasdb.transaction.api.LockAcquisitionException;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionTask;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManager;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManagers;
import com.palantir.atlasdb.transaction.impl.RawTransaction;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManagerImpl;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManagers;
import com.palantir.atlasdb.transaction.impl.TimelockTimestampServiceAdapter;
import com.palantir.atlasdb.transaction.impl.TransactionTables;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.atlasdb.util.JavaSuppliers;
import com.palantir.leader.LeaderElectionService;
import com.palantir.leader.proxy.AwaitingLeadershipProxy;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.InitialisingRemoteLockService;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.SimpleTimeDuration;
import com.palantir.lock.client.LockRefreshingRemoteLockService;
import com.palantir.lock.impl.LegacyTimelockService;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.lock.v2.TimelockService;
import com.palantir.timestamp.InitialisingTimestampService;
import com.palantir.timestamp.TimestampService;
import com.palantir.timestamp.TimestampStoreInvalidator;

public class InitialisingTransactionManager extends ForwardingObject implements SerializableTransactionManager {

    public static final Logger log = LoggerFactory.getLogger(InitialisingTransactionManager.class);
    private static final LockClient LOCK_CLIENT = LockClient.of("atlas instance");
    private static final int RETRY_AFTER_SECONDS = 5;

    private volatile SerializableTransactionManagerImpl delegate = null;
    private Leaders.LocalPaxosServices localPaxosServices = null;
    private final InitialisingRemoteLockService remoteLockService = InitialisingRemoteLockService.create();
    private final InitialisingTimestampService timestampService = InitialisingTimestampService.createUninitialised();
    private final InitialisingPersistentLockService persistentLockService = InitialisingPersistentLockService.create();
    private final InitialisingSweeperService sweeperService = InitialisingSweeperService.create();

    private final ServiceDiscoveringAtlasSupplier atlasFactory;
    private final AtlasDbConfig config;
    private final java.util.function.Supplier<java.util.Optional<AtlasDbRuntimeConfig>> runtimeConfigSupplier;
    private final Set<Schema> schemas;
    private final TransactionManagers.Environment env;
    private final LockServerOptions lockServerOptions;
    private final boolean allowHiddenTableAccess;
    private final String userAgent;

    static Consumer<Runnable> runAsync = task -> {
        Thread thread = new Thread(task);
        thread.setDaemon(true);
        thread.start();
    };

    public InitialisingTransactionManager(
            AtlasDbConfig config,
            java.util.function.Supplier<java.util.Optional<AtlasDbRuntimeConfig>> runtimeConfigSupplier,
            Set<Schema> schemas,
            TransactionManagers.Environment env,
            LockServerOptions lockServerOptions,
            boolean allowHiddenTableAccess,
            String userAgent) {

        atlasFactory = new ServiceDiscoveringAtlasSupplier(config);
        this.config = config;
        this.runtimeConfigSupplier = runtimeConfigSupplier;
        this.schemas = schemas;
        this.env = env;
        this.lockServerOptions = lockServerOptions;
        this.allowHiddenTableAccess = allowHiddenTableAccess;
        this.userAgent = userAgent;

        LockRequest.setDefaultLockTimeout(
                SimpleTimeDuration.of(config.getDefaultLockTimeoutSeconds(), TimeUnit.SECONDS));

        registerEndpoints();

        try {
            initialise();
        } catch (Throwable th) {
            log.info("Synchronous initialisation failed, initialisation will be done asynchronously", th);
            initialiseAsync();
        }
    }

    private void registerEndpoints() {
        if (config.leader().isPresent()) {
            registerTimestampAndLockEndpoints();
            localPaxosServices = Leaders.createAndRegisterLocalServices(env, config.leader().get(), userAgent);
        } else if (!((config.timestamp().isPresent() && config.lock().isPresent()) || config.timelock().isPresent())) {
            registerTimestampAndLockEndpoints();
        }
        env.register(persistentLockService);
        env.register(new CheckAndSetExceptionMapper());
        env.register(sweeperService);
    }

    private void registerTimestampAndLockEndpoints() {
        env.register(remoteLockService);
        env.register(timestampService);
    }

    private void initialiseAsync() {
        runAsync.accept(() -> {
            while (!isInitialized()) {
                try {
                    initialise();
                    // TODO(hsaraogi): catch some other exceptions that shouldn't be thrown to the user.
                } catch (IllegalArgumentException ex) {
                    log.info("Async initialisation failed, the service is in a state which will require "
                            + "manual intervention to start.", ex);
                    break;
                } catch (Throwable th) {
                    log.info("Async initialisation failed, retrying in {} seconds.", RETRY_AFTER_SECONDS, th);
                    Uninterruptibles.sleepUninterruptibly(RETRY_AFTER_SECONDS, TimeUnit.SECONDS);
                }
            }
        });
    }

    private void initialise() {
        KeyValueService rawKvs = atlasFactory.getKeyValueService();

        LockAndTimestampServices lockAndTimestampServices = createLockAndTimestampServices(
                JavaSuppliers.compose(getTimestampClientConfigFunction(), runtimeConfigSupplier),
                () -> LockServiceImpl.create(lockServerOptions),
                atlasFactory::getTimestampService,
                atlasFactory.getTimestampStoreInvalidator());

        KeyValueService kvs = NamespacedKeyValueServices.wrapWithStaticNamespaceMappingKvs(rawKvs);
        kvs = ProfilingKeyValueService.create(kvs, config.getKvsSlowLogThresholdMillis());
        kvs = SweepStatsKeyValueService.create(kvs,
                new TimelockTimestampServiceAdapter(lockAndTimestampServices.timelock()));
        kvs = TracingKeyValueService.create(kvs);
        kvs = AtlasDbMetrics.instrument(KeyValueService.class, kvs,
                MetricRegistry.name(KeyValueService.class, userAgent));
        kvs = ValidatingQueryRewritingKeyValueService.create(kvs);

        TransactionTables.createTables(kvs);

        persistentLockService.initialise(createPersistentLockService(kvs));

        TransactionService transactionService = TransactionServices.createTransactionService(kvs);
        ConflictDetectionManager conflictManager = ConflictDetectionManagers.create(kvs);
        SweepStrategyManager sweepStrategyManager = SweepStrategyManagers.createDefault(kvs);

        Set<Schema> allSchemas = ImmutableSet.<Schema>builder()
                .add(SweepSchema.INSTANCE.getLatestSchema())
                .addAll(schemas)
                .build();
        for (Schema schema : allSchemas) {
            Schemas.createTablesAndIndexes(schema, kvs);
        }

        LoggingArgs.hydrate(kvs.getMetadataForTables());

        CleanupFollower follower = CleanupFollower.create(schemas);

        Cleaner cleaner = new DefaultCleanerBuilder(
                kvs,
                lockAndTimestampServices.timelock(),
                ImmutableList.of(follower),
                transactionService)
                .setBackgroundScrubAggressively(config.backgroundScrubAggressively())
                .setBackgroundScrubBatchSize(config.getBackgroundScrubBatchSize())
                .setBackgroundScrubFrequencyMillis(config.getBackgroundScrubFrequencyMillis())
                .setBackgroundScrubThreads(config.getBackgroundScrubThreads())
                .setPunchIntervalMillis(config.getPunchIntervalMillis())
                .setTransactionReadTimeout(config.getTransactionReadTimeoutMillis())
                .buildCleaner();

        SerializableTransactionManagerImpl transactionManager = new SerializableTransactionManagerImpl(kvs,
                lockAndTimestampServices.timelock(),
                lockAndTimestampServices.lock(),
                transactionService,
                Suppliers.ofInstance(AtlasDbConstraintCheckingMode.FULL_CONSTRAINT_CHECKING_THROWS_EXCEPTIONS),
                conflictManager,
                sweepStrategyManager,
                cleaner,
                allowHiddenTableAccess,
                () -> getAtlasDbRuntimeConfig().transaction().getLockAcquireTimeoutMillis());

        PersistentLockManager persistentLockManager = new PersistentLockManager(
                persistentLockService,
                config.getSweepPersistentLockWaitMillis());

        initializeSweepEndpointAndBackgroundProcess(() -> getAtlasDbRuntimeConfig(),
                kvs,
                transactionService,
                sweepStrategyManager,
                follower,
                transactionManager,
                persistentLockManager);

        delegate = transactionManager;
    }

    private Function<Optional<AtlasDbRuntimeConfig>, TimestampClientConfig> getTimestampClientConfigFunction() {
        return runtimeConfig -> runtimeConfig
                .map(AtlasDbRuntimeConfig::timestampClient)
                .orElse(ImmutableTimestampClientConfig.of());
    }

    @VisibleForTesting
    LockAndTimestampServices createLockAndTimestampServices(
            java.util.function.Supplier<TimestampClientConfig> runtimeTimestampConfigSupplier,
            Supplier<RemoteLockService> lock,
            Supplier<TimestampService> time,
            TimestampStoreInvalidator invalidator) {
        LockAndTimestampServices lockAndTimestampServices =
                createRawServices(lock, time, invalidator);
        return withRequestBatchingTimestampService(
                runtimeTimestampConfigSupplier,
                withRefreshingLockService(lockAndTimestampServices));
    }

    private LockAndTimestampServices withRefreshingLockService(LockAndTimestampServices lockAndTimestampServices) {
        return ImmutableLockAndTimestampServices.builder()
                .from(lockAndTimestampServices)
                .lock(LockRefreshingRemoteLockService.create(lockAndTimestampServices.lock()))
                .build();
    }

    private static LockAndTimestampServices withRequestBatchingTimestampService(
            java.util.function.Supplier<TimestampClientConfig> timestampClientConfigSupplier,
            LockAndTimestampServices lockAndTimestampServices) {
        TimelockService timelockServiceWithBatching =
                DecoratedTimelockServices.createTimelockServiceWithTimestampBatching(
                        lockAndTimestampServices.timelock(), timestampClientConfigSupplier);

        return ImmutableLockAndTimestampServices.builder()
                .from(lockAndTimestampServices)
                .timestamp(new TimelockTimestampServiceAdapter(timelockServiceWithBatching))
                .timelock(timelockServiceWithBatching)
                .build();
    }


    private LockAndTimestampServices createRawServices(
            Supplier<RemoteLockService> lock,
            Supplier<TimestampService> time,
            TimestampStoreInvalidator invalidator) {
        if (config.leader().isPresent()) {
            LockAndTimestampServices services = createRawLeaderServices(config.leader().get(), lock, time);
            remoteLockService.initialise(services.lock());
            timestampService.initialise(services.timestamp());
            return services;
        } else if (config.timestamp().isPresent() && config.lock().isPresent()) {
            LockAndTimestampServices services = TransactionManagers.createRawRemoteServices(config, userAgent);
            remoteLockService.initialise(services.lock());
            timestampService.initialise(services.timestamp());
            return services;
        } else if (config.timelock().isPresent()) {
            TimeLockClientConfig timeLockClientConfig = config.timelock().get();
            TimeLockMigrator.create(timeLockClientConfig, invalidator, userAgent).migrate();
            return createNamespacedRawRemoteServices(timeLockClientConfig);
        } else {
            LockAndTimestampServices services = createRawEmbeddedServices(lock, time);
            remoteLockService.initialise(services.lock());
            timestampService.initialise(services.timestamp());
            return services;
        }
    }

    private LockAndTimestampServices createRawLeaderServices(
            LeaderConfig leaderConfig,
            Supplier<RemoteLockService> lock,
            Supplier<TimestampService> time) {
        // Create local services, that may or may not end up being registered in an environment.
        LeaderElectionService leader = localPaxosServices.leaderElectionService();
        RemoteLockService localLock = AwaitingLeadershipProxy.newProxyInstance(RemoteLockService.class, lock, leader);
        TimestampService localTime = AwaitingLeadershipProxy.newProxyInstance(TimestampService.class, time, leader);

        return TransactionManagers.setupProxyAroundTimeAndLockServices(leaderConfig, userAgent, localPaxosServices,
                localLock, localTime);
    }

    private LockAndTimestampServices createNamespacedRawRemoteServices(TimeLockClientConfig timeLockClientConfig) {
        ServerListConfig namespacedServerListConfig = timeLockClientConfig.toNamespacedServerList();
        return TransactionManagers.getLockAndTimestampServices(namespacedServerListConfig, userAgent);
    }

    private LockAndTimestampServices createRawEmbeddedServices(
            Supplier<RemoteLockService> lock,
            Supplier<TimestampService> time) {
        RemoteLockService lockService = ServiceCreator.createInstrumentedService(lock.get(),
                RemoteLockService.class,
                userAgent);
        TimestampService timeService = ServiceCreator.createInstrumentedService(time.get(),
                TimestampService.class,
                userAgent);
        return ImmutableLockAndTimestampServices.builder()
                .lock(lockService)
                .timestamp(timeService)
                .timelock(new LegacyTimelockService(timeService, lockService, LOCK_CLIENT))
                .build();
    }

    private PersistentLockService createPersistentLockService(KeyValueService kvs) {
        if (!kvs.supportsCheckAndSet()) {
            return new NoOpPersistentLockService();
        }
        return KvsBackedPersistentLockService.create(kvs);
    }

    private void initializeSweepEndpointAndBackgroundProcess(
            java.util.function.Supplier<AtlasDbRuntimeConfig> runtimeConfig,
            KeyValueService kvs,
            TransactionService transactionService,
            SweepStrategyManager sweepStrategyManager,
            CleanupFollower follower,
            SerializableTransactionManagerImpl transactionManager,
            PersistentLockManager persistentLockManager) {
        CellsSweeper cellsSweeper = new CellsSweeper(
                transactionManager,
                kvs,
                persistentLockManager,
                ImmutableList.of(follower));
        SweepTaskRunner sweepRunner = new SweepTaskRunner(
                kvs,
                transactionManager::getUnreadableTimestamp,
                transactionManager::getImmutableTimestamp,
                transactionService,
                sweepStrategyManager,
                cellsSweeper);
        BackgroundSweeperPerformanceLogger sweepPerfLogger = new NoOpBackgroundSweeperPerformanceLogger();
        Supplier<SweepBatchConfig> sweepBatchConfig =
                Suppliers.ofInstance(getSweepBatchConfig(getAtlasDbRuntimeConfig().sweep()));
        SweepMetrics sweepMetrics = new SweepMetrics();

        SpecificTableSweeper specificTableSweeper = SpecificTableSweeper.create(
                transactionManager,
                kvs,
                sweepRunner,
                sweepBatchConfig,
                SweepTableFactory.of(),
                sweepPerfLogger,
                sweepMetrics);

        sweeperService.initialise(new SweeperServiceImpl(specificTableSweeper));

        BackgroundSweeperImpl backgroundSweeper = BackgroundSweeperImpl.create(
                () -> getAtlasDbRuntimeConfig().sweep().enabled(),
                () -> getAtlasDbRuntimeConfig().sweep().pauseMillis(),
                persistentLockManager,
                specificTableSweeper);

        transactionManager.registerClosingCallback(backgroundSweeper::shutdown);
        backgroundSweeper.runInBackground();
    }

    private AtlasDbRuntimeConfig getAtlasDbRuntimeConfig() {
        return runtimeConfigSupplier.get().orElse(AtlasDbRuntimeConfig.defaultRuntimeConfig());
    }

    private static SweepBatchConfig getSweepBatchConfig(SweepConfig sweepConfig) {
        return ImmutableSweepBatchConfig.builder()
                .maxCellTsPairsToExamine(sweepConfig.readLimit())
                .candidateBatchSize(sweepConfig.candidateBatchHint())
                .deleteBatchSize(sweepConfig.deleteBatchHint())
                .build();
    }

    @Override
    public RawTransaction setupRunTaskWithLocksThrowOnConflict(Iterable<LockRefreshToken> lockTokens) {
        return getDelegate().setupRunTaskWithLocksThrowOnConflict(lockTokens);
    }

    @Override
    public <T, E extends Exception> T finishRunTaskWithLockThrowOnConflict(RawTransaction tx,
            TransactionTask<T, E> task) throws E, TransactionFailedRetriableException {
        return getDelegate().finishRunTaskWithLockThrowOnConflict(tx, task);
    }

    @Override
    public void registerClosingCallback(Runnable closingCallback) {
        getDelegate().registerClosingCallback(closingCallback);
    }

    @Override
    public Cleaner getCleaner() {
        return getDelegate().getCleaner();
    }

    @Override
    public KeyValueService getKeyValueService() {
        return getDelegate().getKeyValueService();
    }

    @Override
    public TimestampService getTimestampService() {
        return getDelegate().getTimestampService();
    }

    @Override
    public <T, E extends Exception> T runTaskWithRetry(TransactionTask<T, E> task) throws E {
        return getDelegate().runTaskWithRetry(task);
    }

    @Override
    public <T, E extends Exception> T runTaskThrowOnConflict(TransactionTask<T, E> task)
            throws E, TransactionFailedRetriableException {
        return getDelegate().runTaskThrowOnConflict(task);
    }

    @Override
    public <T, E extends Exception> T runTaskReadOnly(TransactionTask<T, E> task) throws E {
        checkInitialised();
        return delegate.runTaskReadOnly(task);
    }

    @Override
    public long getImmutableTimestamp() {
        return getDelegate().getImmutableTimestamp();
    }

    @Override
    public KeyValueServiceStatus getKeyValueServiceStatus() {
        checkInitialised();
        return delegate.getKeyValueServiceStatus();
    }

    @Override
    public long getUnreadableTimestamp() {
        return getDelegate().getUnreadableTimestamp();
    }

    @Override
    public void clearTimestampCache() {
        getDelegate().clearTimestampCache();
    }

    @Override
    public void close() {
        getDelegate().close();
    }

    @Override
    public <T, E extends Exception> T runTaskWithLocksWithRetry(Supplier<LockRequest> lockSupplier,
            LockAwareTransactionTask<T, E> task) throws E, InterruptedException, LockAcquisitionException {
        return getDelegate().runTaskWithLocksWithRetry(lockSupplier, task);
    }

    @Override
    public <T, E extends Exception> T runTaskWithLocksWithRetry(Iterable<HeldLocksToken> lockTokens,
            Supplier<LockRequest> lockSupplier, LockAwareTransactionTask<T, E> task)
            throws E, InterruptedException, LockAcquisitionException {
        return getDelegate().runTaskWithLocksWithRetry(lockTokens, lockSupplier, task);
    }

    @Override
    public <T, E extends Exception> T runTaskWithLocksThrowOnConflict(Iterable<HeldLocksToken> lockTokens,
            LockAwareTransactionTask<T, E> task) throws E, TransactionFailedRetriableException {
        return getDelegate().runTaskWithLocksThrowOnConflict(lockTokens, task);
    }

    @Override
    public RemoteLockService getLockService() {
        return getDelegate().getLockService();
    }

    @Override
    public TimelockService getTimelockService() {
        return getDelegate().getTimelockService();
    }

    private SerializableTransactionManagerImpl getDelegate() {
        return (SerializableTransactionManagerImpl) delegate();
    }

    @Override
    protected Object delegate() {
        checkInitialised();
        return delegate;
    }

    private void checkInitialised() {
        if (!isInitialized()) {
            throw new IllegalStateException("The transaction manager is not initialized.");
        }
    }

    public boolean isInitialized() {
        return delegate != null;
    }
}
