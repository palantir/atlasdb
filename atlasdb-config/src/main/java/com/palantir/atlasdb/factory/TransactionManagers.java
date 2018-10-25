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
package com.palantir.atlasdb.factory;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.ws.rs.ClientErrorException;

import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.async.initializer.AsyncInitializer;
import com.palantir.async.initializer.Callback;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cache.TimestampCache;
import com.palantir.atlasdb.cleaner.CleanupFollower;
import com.palantir.atlasdb.cleaner.DefaultCleanerBuilder;
import com.palantir.atlasdb.cleaner.Follower;
import com.palantir.atlasdb.cleaner.api.Cleaner;
import com.palantir.atlasdb.compact.BackgroundCompactor;
import com.palantir.atlasdb.compact.CompactorConfig;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbRuntimeConfig;
import com.palantir.atlasdb.config.ImmutableAtlasDbConfig;
import com.palantir.atlasdb.config.ImmutableLeaderRuntimeConfig;
import com.palantir.atlasdb.config.ImmutableServerListConfig;
import com.palantir.atlasdb.config.ImmutableTimeLockClientConfig;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.config.LeaderRuntimeConfig;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.config.ServerListConfigs;
import com.palantir.atlasdb.config.SweepConfig;
import com.palantir.atlasdb.config.TargetedSweepInstallConfig;
import com.palantir.atlasdb.config.TargetedSweepRuntimeConfig;
import com.palantir.atlasdb.config.TimeLockClientConfig;
import com.palantir.atlasdb.factory.Leaders.LocalPaxosServices;
import com.palantir.atlasdb.factory.startup.ConsistencyCheckRunner;
import com.palantir.atlasdb.factory.startup.TimeLockMigrator;
import com.palantir.atlasdb.factory.timestamp.FreshTimestampSupplierAdapter;
import com.palantir.atlasdb.http.AtlasDbFeignTargetFactory;
import com.palantir.atlasdb.http.UserAgents;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.ProfilingKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.SweepStatsKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.TracingKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.ValidatingQueryRewritingKeyValueService;
import com.palantir.atlasdb.logging.KvsProfilingLogger;
import com.palantir.atlasdb.memory.InMemoryAtlasDbConfig;
import com.palantir.atlasdb.persistentlock.CheckAndSetExceptionMapper;
import com.palantir.atlasdb.persistentlock.KvsBackedPersistentLockService;
import com.palantir.atlasdb.persistentlock.NoOpPersistentLockService;
import com.palantir.atlasdb.persistentlock.PersistentLockService;
import com.palantir.atlasdb.schema.generated.SweepTableFactory;
import com.palantir.atlasdb.sweep.AdjustableSweepBatchConfigSource;
import com.palantir.atlasdb.sweep.BackgroundSweeperImpl;
import com.palantir.atlasdb.sweep.BackgroundSweeperPerformanceLogger;
import com.palantir.atlasdb.sweep.CellsSweeper;
import com.palantir.atlasdb.sweep.ImmutableSweepBatchConfig;
import com.palantir.atlasdb.sweep.NoOpBackgroundSweeperPerformanceLogger;
import com.palantir.atlasdb.sweep.PersistentLockManager;
import com.palantir.atlasdb.sweep.SpecificTableSweeper;
import com.palantir.atlasdb.sweep.SweepBatchConfig;
import com.palantir.atlasdb.sweep.SweepTaskRunner;
import com.palantir.atlasdb.sweep.SweeperServiceImpl;
import com.palantir.atlasdb.sweep.metrics.LegacySweepMetrics;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.sweep.queue.TargetedSweeper;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManager;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManagers;
import com.palantir.atlasdb.transaction.impl.InstrumentedTimelockService;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManagers;
import com.palantir.atlasdb.transaction.impl.TimelockTimestampServiceAdapter;
import com.palantir.atlasdb.transaction.impl.consistency.ImmutableTimestampCorroborationConsistencyCheck;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.common.annotation.Output;
import com.palantir.leader.LeaderElectionService;
import com.palantir.leader.PingableLeader;
import com.palantir.leader.proxy.AwaitingLeadershipProxy;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.LockService;
import com.palantir.lock.SimpleTimeDuration;
import com.palantir.lock.client.LockRefreshingLockService;
import com.palantir.lock.client.TimeLockClient;
import com.palantir.lock.impl.LegacyTimelockService;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.lock.v2.TimelockService;
import com.palantir.logsafe.SafeArg;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampService;
import com.palantir.timestamp.TimestampStoreInvalidator;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import com.palantir.util.OptionalResolver;

@Value.Immutable
@Value.Style(stagedBuilder = true)
public abstract class TransactionManagers {
    private static final int LOGGING_INTERVAL = 60;
    private static final Logger log = LoggerFactory.getLogger(TransactionManagers.class);

    public static final LockClient LOCK_CLIENT = LockClient.of("atlas instance");

    abstract AtlasDbConfig config();

    @Value.Default
    Supplier<Optional<AtlasDbRuntimeConfig>> runtimeConfigSupplier() {
        return Optional::empty;
    }

    abstract Set<Schema> schemas();

    @Value.Default
    Consumer<Object> registrar() {
        return resource -> { };
    }

    @Value.Default
    LockServerOptions lockServerOptions() {
        return LockServerOptions.DEFAULT;
    }

    @Value.Default
    boolean allowHiddenTableAccess() {
        return false;
    }

    @Value.Default
    boolean validateLocksOnReads() {
        return true;
    }

    abstract String userAgent();

    abstract MetricRegistry globalMetricsRegistry();

    abstract TaggedMetricRegistry globalTaggedMetricRegistry();

    /**
     * The callback Runnable will be run when the TransactionManager is successfully initialized. The
     * TransactionManager will stay uninitialized and continue to throw for all other purposes until the callback
     * returns at which point it will become initialized. If asynchronous initialization is disabled, the callback will
     * be run just before the TM is returned.
     *
     * Note that if the callback blocks forever, the TransactionManager will never become initialized, and calling its
     * close() method will block forever as well. If the callback init() fails, and its cleanup() method throws,
     * the TransactionManager will not become initialized and it will be closed.
     */
    @Value.Default
    Callback<TransactionManager> asyncInitializationCallback() {
        return Callback.noOp();
    }

    public static ImmutableTransactionManagers.ConfigBuildStage builder() {
        return ImmutableTransactionManagers.builder();
    }

    @VisibleForTesting
    static Consumer<Runnable> runAsync = task -> {
        Thread thread = new Thread(task);
        thread.setDaemon(true);
        thread.start();
    };

    /**
     * Accepts a single {@link Schema}.
     *
     * @see TransactionManagers#createInMemory(Set)
     */
    public static TransactionManager createInMemory(Schema schema) {
        return createInMemory(ImmutableSet.of(schema));
    }

    /**
     * Create a {@link TransactionManager} backed by an
     * {@link com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService}. This should be used for testing
     * purposes only.
     */
    public static TransactionManager createInMemory(Set<Schema> schemas) {
        AtlasDbConfig config = ImmutableAtlasDbConfig.builder().keyValueService(new InMemoryAtlasDbConfig()).build();
        return builder()
                .config(config)
                .userAgent(UserAgents.DEFAULT_USER_AGENT)
                .globalMetricsRegistry(new MetricRegistry())
                .globalTaggedMetricRegistry(DefaultTaggedMetricRegistry.getDefault())
                .addAllSchemas(schemas)
                .build()
                .serializable();
    }

    @JsonIgnore
    @Value.Derived
    public TransactionManager serializable() {
        List<AutoCloseable> closeables = Lists.newArrayList();

        try {
            return serializableInternal(closeables);
        } catch (Throwable throwable) {
            List<String> closeablesClasses = closeables.stream()
                    .map(autoCloseable -> autoCloseable.getClass().toString())
                    .collect(Collectors.toList());

            log.warn("Exception thrown when creating transaction manager. "
                    + "Closing previously opened resources: {}", SafeArg.of("classes", closeablesClasses.toString()),
                    throwable);

            closeables.forEach(autoCloseable -> {
                try {
                    autoCloseable.close();
                } catch (Exception ex) {
                    log.info("Error closing {}", SafeArg.of("class", autoCloseable.getClass().toString()), ex);
                }
            });
            throw throwable;
        }
    }

    @SuppressWarnings("MethodLength")
    private TransactionManager serializableInternal(@Output List<AutoCloseable> closeables) {
        MetricsManager metricsManager = MetricsManagers.of(globalMetricsRegistry(), globalTaggedMetricRegistry());

        AtlasDbRuntimeConfig defaultRuntime = AtlasDbRuntimeConfig.defaultRuntimeConfig();
        Supplier<AtlasDbRuntimeConfig> runtimeConfigSupplier =
                () -> runtimeConfigSupplier().get().orElse(defaultRuntime);

        FreshTimestampSupplierAdapter adapter = new FreshTimestampSupplierAdapter();
        ServiceDiscoveringAtlasSupplier atlasFactory = new ServiceDiscoveringAtlasSupplier(metricsManager,
                        config().keyValueService(),
                        Suppliers.compose(AtlasDbRuntimeConfig::keyValueService, runtimeConfigSupplier::get),
                        config().leader(), config().namespace(), Optional.empty(), config().initializeAsync(),
                         adapter);

        LockRequest.setDefaultLockTimeout(
                SimpleTimeDuration.of(config().getDefaultLockTimeoutSeconds(), TimeUnit.SECONDS));

        com.google.common.base.Supplier<TimestampService> timestampSupplier =
                Suppliers.memoize(atlasFactory::getTimestampService);
        com.google.common.base.Supplier<TimestampManagementService> managementSupplier =
                Suppliers.compose(atlasFactory::getTimestampManagementService, timestampSupplier);

        LockAndTimestampServices lockAndTimestampServices = createLockAndTimestampServices(
                metricsManager,
                config(),
                runtimeConfigSupplier,
                registrar(),
                () -> LockServiceImpl.create(lockServerOptions()),
                timestampSupplier,
                managementSupplier,
                atlasFactory.getTimestampStoreInvalidator(),
                userAgent());
        adapter.setTimestampService(lockAndTimestampServices.timestamp());

        KvsProfilingLogger.setSlowLogThresholdMillis(config().getKvsSlowLogThresholdMillis());

        Supplier<SweepConfig> sweepConfig = Suppliers.compose(AtlasDbRuntimeConfig::sweep, runtimeConfigSupplier::get);

        KeyValueService keyValueService = initializeCloseable(() -> {
            KeyValueService kvs = atlasFactory.getKeyValueService();
            kvs = ProfilingKeyValueService.create(kvs);

            // If we are writing to the sweep queue, then we do not need to use SweepStatsKVS to record modifications.
            if (!config().targetedSweep().enableSweepQueueWrites()) {
                kvs = SweepStatsKeyValueService.create(kvs,
                        new TimelockTimestampServiceAdapter(lockAndTimestampServices.timelock()),
                        Suppliers.compose(SweepConfig::writeThreshold, sweepConfig::get),
                        Suppliers.compose(SweepConfig::writeSizeThreshold, sweepConfig::get)
                );
            }

            kvs = TracingKeyValueService.create(kvs);
            kvs = AtlasDbMetrics.instrument(metricsManager.getRegistry(),
                    KeyValueService.class,
                    kvs,
                    MetricRegistry.name(KeyValueService.class));
            return ValidatingQueryRewritingKeyValueService.create(kvs);
        }, closeables);

        TransactionManagersInitializer initializer = TransactionManagersInitializer.createInitialTables(
                keyValueService, schemas(), config().initializeAsync());
        PersistentLockService persistentLockService = createAndRegisterPersistentLockService(
                keyValueService, registrar(), config().initializeAsync());

        TransactionService transactionService = AtlasDbMetrics.instrument(
                metricsManager.getRegistry(),
                TransactionService.class,
                TransactionServices.createTransactionService(keyValueService));
        ConflictDetectionManager conflictManager = ConflictDetectionManagers.create(keyValueService);
        SweepStrategyManager sweepStrategyManager = SweepStrategyManagers.createDefault(keyValueService);

        CleanupFollower follower = CleanupFollower.create(schemas());

        Cleaner cleaner = initializeCloseable(() ->
                        new DefaultCleanerBuilder(keyValueService, lockAndTimestampServices.timelock(),
                                        ImmutableList.of(follower), transactionService)
                                .setBackgroundScrubAggressively(config().backgroundScrubAggressively())
                                .setBackgroundScrubBatchSize(config().getBackgroundScrubBatchSize())
                                .setBackgroundScrubFrequencyMillis(config().getBackgroundScrubFrequencyMillis())
                                .setBackgroundScrubThreads(config().getBackgroundScrubThreads())
                                .setPunchIntervalMillis(config().getPunchIntervalMillis())
                                .setTransactionReadTimeout(config().getTransactionReadTimeoutMillis())
                                .setInitializeAsync(config().initializeAsync())
                                .buildCleaner(),
                closeables);

        MultiTableSweepQueueWriter targetedSweep = initializeCloseable(
                () -> uninitializedTargetedSweeper(metricsManager, config().targetedSweep(), follower,
                        Suppliers.compose(AtlasDbRuntimeConfig::targetedSweep, runtimeConfigSupplier::get)),
                closeables);

        Callback<TransactionManager> callbacks = new Callback.CallChain<>(
                timelockConsistencyCheckCallback(config(), runtimeConfigSupplier.get(), lockAndTimestampServices),
                targetedSweep.singleAttemptCallback(),
                new DeprecatedTablesCleaner(schemas()),
                asyncInitializationCallback());

        TransactionManager transactionManager = initializeCloseable(
                () -> SerializableTransactionManager.create(
                        metricsManager,
                        keyValueService,
                        lockAndTimestampServices.timelock(),
                        lockAndTimestampServices.timestampManagement(),
                        lockAndTimestampServices.lock(),
                        transactionService,
                        Suppliers.ofInstance(AtlasDbConstraintCheckingMode.FULL_CONSTRAINT_CHECKING_THROWS_EXCEPTIONS),
                        conflictManager,
                        sweepStrategyManager,
                        cleaner,
                        () -> areTransactionManagerInitializationPrerequisitesSatisfied(
                                initializer,
                                lockAndTimestampServices),
                        allowHiddenTableAccess(),
                        config().keyValueService().concurrentGetRangesThreadPoolSize(),
                        config().keyValueService().defaultGetRangesConcurrency(),
                        config().initializeAsync(),
                        new TimestampCache(metricsManager.getRegistry(),
                                () -> runtimeConfigSupplier.get().getTimestampCacheSize()),
                        targetedSweep,
                        callbacks,
                        validateLocksOnReads(),
                        () -> runtimeConfigSupplier.get().transaction()),
                closeables);

        TransactionManager instrumentedTransactionManager =
                AtlasDbMetrics.instrument(metricsManager.getRegistry(), TransactionManager.class, transactionManager);

        instrumentedTransactionManager.registerClosingCallback(lockAndTimestampServices::close);
        instrumentedTransactionManager.registerClosingCallback(targetedSweep::close);

        PersistentLockManager persistentLockManager = initializeCloseable(
                () -> new PersistentLockManager(
                        metricsManager, persistentLockService, config().getSweepPersistentLockWaitMillis()),
                closeables);
        initializeCloseable(
                () -> initializeSweepEndpointAndBackgroundProcess(
                        metricsManager,
                        config(),
                        runtimeConfigSupplier,
                        registrar(),
                        keyValueService,
                        transactionService,
                        sweepStrategyManager,
                        follower,
                        instrumentedTransactionManager,
                        persistentLockManager),
                closeables);
        initializeCloseable(
                initializeCompactBackgroundProcess(
                        metricsManager,
                        lockAndTimestampServices,
                        keyValueService,
                        instrumentedTransactionManager,
                        Suppliers.compose(AtlasDbRuntimeConfig::compact, runtimeConfigSupplier::get)),
                closeables);

        return instrumentedTransactionManager;
    }

    private Optional<BackgroundCompactor> initializeCompactBackgroundProcess(
            MetricsManager metricsManager,
            LockAndTimestampServices lockAndTimestampServices,
            KeyValueService keyValueService,
            TransactionManager transactionManager,
            Supplier<CompactorConfig> compactorConfigSupplier) {
        Optional<BackgroundCompactor> backgroundCompactorOptional = BackgroundCompactor.createAndRun(
                metricsManager,
                transactionManager,
                keyValueService,
                lockAndTimestampServices.lock(),
                compactorConfigSupplier);

        backgroundCompactorOptional.ifPresent(backgroundCompactor ->
                transactionManager.registerClosingCallback(backgroundCompactor::close));

        return backgroundCompactorOptional;
    }

    private <T extends AutoCloseable> T initializeCloseable(
            Supplier<T> closeableSupplier, @Output List<AutoCloseable> closeables) {
        T ret = closeableSupplier.get();
        closeables.add(ret);
        return ret;
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private <T extends AutoCloseable> Optional<T> initializeCloseable(
            Optional<T> closeableOptional, @Output List<AutoCloseable> closeables) {
        closeableOptional.ifPresent(closeables::add);
        return closeableOptional;
    }

    private static boolean areTransactionManagerInitializationPrerequisitesSatisfied(
            AsyncInitializer initializer,
            LockAndTimestampServices lockAndTimestampServices) {
        return initializer.isInitialized() && timeLockMigrationCompleteIfNeeded(lockAndTimestampServices);
    }

    @VisibleForTesting
    static boolean timeLockMigrationCompleteIfNeeded(LockAndTimestampServices lockAndTimestampServices) {
        return lockAndTimestampServices.migrator().map(AsyncInitializer::isInitialized).orElse(true);
    }

    private static BackgroundSweeperImpl initializeSweepEndpointAndBackgroundProcess(
            MetricsManager metricsManager,
            AtlasDbConfig config,
            Supplier<AtlasDbRuntimeConfig> runtimeConfigSupplier,
            Consumer<Object> env,
            KeyValueService kvs,
            TransactionService transactionService,
            SweepStrategyManager sweepStrategyManager,
            CleanupFollower follower,
            TransactionManager transactionManager,
            PersistentLockManager persistentLockManager) {
        CellsSweeper cellsSweeper = new CellsSweeper(
                transactionManager,
                kvs,
                persistentLockManager,
                ImmutableList.of(follower));

        LegacySweepMetrics sweepMetrics = new LegacySweepMetrics(metricsManager.getRegistry());

        SweepTaskRunner sweepRunner = new SweepTaskRunner(
                kvs,
                transactionManager::getUnreadableTimestamp,
                transactionManager::getImmutableTimestamp,
                transactionService,
                sweepStrategyManager,
                cellsSweeper,
                sweepMetrics);
        BackgroundSweeperPerformanceLogger sweepPerfLogger = new NoOpBackgroundSweeperPerformanceLogger();
        AdjustableSweepBatchConfigSource sweepBatchConfigSource = AdjustableSweepBatchConfigSource.create(
                metricsManager,
                () -> getSweepBatchConfig(runtimeConfigSupplier.get().sweep()));

        SpecificTableSweeper specificTableSweeper = initializeSweepEndpoint(
                env,
                kvs,
                transactionManager,
                sweepRunner,
                sweepPerfLogger,
                sweepMetrics,
                config.initializeAsync(),
                sweepBatchConfigSource);

        BackgroundSweeperImpl backgroundSweeper = BackgroundSweeperImpl.create(
                metricsManager,
                sweepBatchConfigSource,
                () -> runtimeConfigSupplier.get().sweep().enabled(),
                () -> runtimeConfigSupplier.get().sweep().sweepThreads(),
                () -> runtimeConfigSupplier.get().sweep().pauseMillis(),
                () -> runtimeConfigSupplier.get().sweep().sweepPriorityOverrides(),
                persistentLockManager,
                specificTableSweeper);

        transactionManager.registerClosingCallback(backgroundSweeper::shutdown);
        backgroundSweeper.runInBackground();

        return backgroundSweeper;
    }

    private static SpecificTableSweeper initializeSweepEndpoint(
            Consumer<Object> env,
            KeyValueService kvs,
            TransactionManager transactionManager,
            SweepTaskRunner sweepRunner,
            BackgroundSweeperPerformanceLogger sweepPerfLogger,
            LegacySweepMetrics sweepMetrics,
            boolean initializeAsync,
            AdjustableSweepBatchConfigSource sweepBatchConfigSource) {
        SpecificTableSweeper specificTableSweeper = SpecificTableSweeper.create(
                transactionManager,
                kvs,
                sweepRunner,
                SweepTableFactory.of(),
                sweepPerfLogger,
                sweepMetrics,
                initializeAsync);
        env.accept(new SweeperServiceImpl(specificTableSweeper, sweepBatchConfigSource));
        return specificTableSweeper;
    }

    private static SweepBatchConfig getSweepBatchConfig(SweepConfig sweepConfig) {
        return ImmutableSweepBatchConfig.builder()
                .maxCellTsPairsToExamine(sweepConfig.readLimit())
                .candidateBatchSize(sweepConfig.candidateBatchHint()
                        .orElse(AtlasDbConstants.DEFAULT_SWEEP_CANDIDATE_BATCH_HINT))
                .deleteBatchSize(sweepConfig.deleteBatchHint())
                .build();
    }

    private static PersistentLockService createAndRegisterPersistentLockService(
            KeyValueService kvs,
            Consumer<Object> env,
            boolean initializeAsync) {
        if (!kvs.supportsCheckAndSet()) {
            return new NoOpPersistentLockService();
        }

        PersistentLockService pls = KvsBackedPersistentLockService.create(kvs, initializeAsync);
        env.accept(pls);
        env.accept(new CheckAndSetExceptionMapper());
        return pls;
    }

    private static Callback<TransactionManager> timelockConsistencyCheckCallback(
            AtlasDbConfig atlasDbConfig,
            AtlasDbRuntimeConfig initialRuntimeConfig,
            LockAndTimestampServices lockAndTimestampServices) {
        if (isUsingTimeLock(atlasDbConfig, initialRuntimeConfig)) {
            // Only do the consistency check if we're using TimeLock.
            // This avoids a bootstrapping problem with leader-block services without async initialisation,
            // where you need a working timestamp service to check consistency, you need to check consistency
            // before you can return a TM, you need to return a TM to listen on ports, and you need to listen on
            // ports in order to get a working timestamp service.
            return ConsistencyCheckRunner.create(
                    ImmutableTimestampCorroborationConsistencyCheck.builder()
                            .conservativeBound(TransactionManager::getUnreadableTimestamp)
                            .freshTimestampSource(unused -> lockAndTimestampServices.timelock().getFreshTimestamp())
                            .build());
        }
        return Callback.noOp();
    }

    private static boolean isUsingTimeLock(AtlasDbConfig atlasDbConfig, AtlasDbRuntimeConfig runtimeConfig) {
        return atlasDbConfig.timelock().isPresent() || runtimeConfig.timelockRuntime().isPresent();
    }

    /**
     * This method should not be used directly. It remains here to support the AtlasDB-Dagger module and the CLIs, but
     * may be removed at some point in the future.
     *
     * @deprecated Not intended for public use outside of the AtlasDB CLIs
     */
    @Deprecated
    public static LockAndTimestampServices createLockAndTimestampServicesForCli(
            MetricsManager metricsManager,
            AtlasDbConfig config,
            java.util.function.Supplier<AtlasDbRuntimeConfig> runtimeConfigSupplier,
            Consumer<Object> env,
            com.google.common.base.Supplier<LockService> lock,
            com.google.common.base.Supplier<TimestampService> time,
            com.google.common.base.Supplier<TimestampManagementService> timeManagement,
            TimestampStoreInvalidator invalidator,
            String userAgent) {
        LockAndTimestampServices lockAndTimestampServices =
                createRawInstrumentedServices(
                        metricsManager,
                        config,
                        runtimeConfigSupplier,
                        env,
                        lock,
                        time,
                        timeManagement,
                        invalidator,
                        userAgent);
        TimeLockClient timeLockClient = TimeLockClient.withSynchronousUnlocker(lockAndTimestampServices.timelock());
        return ImmutableLockAndTimestampServices.builder()
                .from(lockAndTimestampServices)
                .timelock(timeLockClient)
                .lock(LockRefreshingLockService.create(lockAndTimestampServices.lock()))
                .close(timeLockClient::close)
                .build();
    }

    @VisibleForTesting
    static LockAndTimestampServices createLockAndTimestampServices(
            MetricsManager metricsManager,
            AtlasDbConfig config,
            java.util.function.Supplier<AtlasDbRuntimeConfig> runtimeConfigSupplier,
            Consumer<Object> env,
            com.google.common.base.Supplier<LockService> lock,
            com.google.common.base.Supplier<TimestampService> time,
            com.google.common.base.Supplier<TimestampManagementService> timeManagement,
            TimestampStoreInvalidator invalidator,
            String userAgent) {
        LockAndTimestampServices lockAndTimestampServices = createRawInstrumentedServices(
                metricsManager,
                config,
                runtimeConfigSupplier,
                env,
                lock,
                time,
                timeManagement,
                invalidator,
                userAgent);
        return withMetrics(metricsManager, withRefreshingLockService(lockAndTimestampServices));
    }

    private static LockAndTimestampServices withRefreshingLockService(
            LockAndTimestampServices lockAndTimestampServices) {
        TimeLockClient timeLockClient = TimeLockClient.createDefault(lockAndTimestampServices.timelock());
        return ImmutableLockAndTimestampServices.builder()
                .from(lockAndTimestampServices)
                .timestamp(new TimelockTimestampServiceAdapter(timeLockClient))
                .timelock(timeLockClient)
                .lock(LockRefreshingLockService.create(lockAndTimestampServices.lock()))
                .close(timeLockClient::close)
                .build();
    }

    private static LockAndTimestampServices withMetrics(
            MetricsManager metricsManager,
            LockAndTimestampServices lockAndTimestampServices) {
        TimelockService timelockServiceWithBatching = lockAndTimestampServices.timelock();
        TimelockService instrumentedTimelockService = new InstrumentedTimelockService(
                timelockServiceWithBatching,
                metricsManager.getRegistry());

        return ImmutableLockAndTimestampServices.builder()
                .from(lockAndTimestampServices)
                .timestamp(new TimelockTimestampServiceAdapter(instrumentedTimelockService))
                .timelock(instrumentedTimelockService)
                .build();
    }

    private static LockAndTimestampServices createRawInstrumentedServices(
            MetricsManager metricsManager,
            AtlasDbConfig config,
            Supplier<AtlasDbRuntimeConfig> runtimeConfigSupplier,
            Consumer<Object> env,
            com.google.common.base.Supplier<LockService> lock,
            com.google.common.base.Supplier<TimestampService> time,
            com.google.common.base.Supplier<TimestampManagementService> timeManagement,
            TimestampStoreInvalidator invalidator,
            String userAgent) {
        AtlasDbRuntimeConfig initialRuntimeConfig = runtimeConfigSupplier.get();
        assertNoSpuriousTimeLockBlockInRuntimeConfig(config, initialRuntimeConfig);
        if (config.leader().isPresent()) {
            return createRawLeaderServices(
                    metricsManager, config.leader().get(), env, lock, time, timeManagement, userAgent);
        } else if (config.timestamp().isPresent() && config.lock().isPresent()) {
            return createRawRemoteServices(metricsManager, config, userAgent);
        } else if (isUsingTimeLock(config, initialRuntimeConfig)) {
            return createRawServicesFromTimeLock(metricsManager, config, runtimeConfigSupplier, invalidator, userAgent);
        } else {
            return createRawEmbeddedServices(metricsManager, env, lock, time, timeManagement);
        }
    }

    private static void assertNoSpuriousTimeLockBlockInRuntimeConfig(
            AtlasDbConfig config,
            AtlasDbRuntimeConfig initialRuntimeConfig) {
        // Note: The other direction (timelock install config without a runtime block) should be maintained for
        // backwards compatibility.
        if (remoteTimestampAndLockOrLeaderBlocksPresent(config) && initialRuntimeConfig.timelockRuntime().isPresent()) {
            throw new IllegalStateException("Found a service configured not to use timelock, with a timelock"
                    + " block in the runtime config! This is unexpected. If you wish to use non-timelock services,"
                    + " please remove the timelock block from the runtime config; if you wish to use timelock,"
                    + " please remove the leader, remote timestamp or remote lock configuration blocks.");
        }
    }

    private static boolean remoteTimestampAndLockOrLeaderBlocksPresent(AtlasDbConfig config) {
        return (config.timestamp().isPresent() && config.lock().isPresent()) || config.leader().isPresent();
    }

    private static LockAndTimestampServices createRawServicesFromTimeLock(
            MetricsManager metricsManager,
            AtlasDbConfig config,
            Supplier<AtlasDbRuntimeConfig> runtimeConfigSupplier,
            TimestampStoreInvalidator invalidator,
            String userAgent) {
        Supplier<ServerListConfig> serverListConfigSupplier =
                getServerListConfigSupplierForTimeLock(config, runtimeConfigSupplier);
        TimeLockMigrator migrator =
                TimeLockMigrator.create(metricsManager,
                        serverListConfigSupplier, invalidator, userAgent, config.initializeAsync());
        migrator.migrate(); // This can proceed async if config.initializeAsync() was set
        return ImmutableLockAndTimestampServices.copyOf(
                getLockAndTimestampServices(metricsManager, serverListConfigSupplier, userAgent))
                .withMigrator(migrator);
    }

    private static Supplier<ServerListConfig> getServerListConfigSupplierForTimeLock(
            AtlasDbConfig config,
            Supplier<AtlasDbRuntimeConfig> runtimeConfigSupplier) {
        Preconditions.checkState(!remoteTimestampAndLockOrLeaderBlocksPresent(config),
                "Cannot create raw services from timelock with another source of timestamps/locks configured!");
        TimeLockClientConfig clientConfig = config.timelock().orElse(ImmutableTimeLockClientConfig.builder().build());
        String resolvedClient = OptionalResolver.resolve(clientConfig.client(), config.namespace());
        return () -> ServerListConfigs.parseInstallAndRuntimeConfigs(
                clientConfig,
                () -> runtimeConfigSupplier.get().timelockRuntime(),
                resolvedClient);
    }

    private static LockAndTimestampServices getLockAndTimestampServices(
            MetricsManager metricsManager,
            Supplier<ServerListConfig> timelockServerListConfig,
            String userAgent) {
        LockService lockService = new ServiceCreator<>(metricsManager, LockService.class, userAgent)
                .applyDynamic(timelockServerListConfig);
        TimelockService timelockService = new ServiceCreator<>(metricsManager, TimelockService.class, userAgent)
                .applyDynamic(timelockServerListConfig);
        TimestampManagementService timestampManagementService =
                new ServiceCreator<>(metricsManager, TimestampManagementService.class, userAgent)
                        .applyDynamic(timelockServerListConfig);

        return ImmutableLockAndTimestampServices.builder()
                .lock(lockService)
                .timestamp(new TimelockTimestampServiceAdapter(timelockService))
                .timestampManagement(timestampManagementService)
                .timelock(timelockService)
                .build();
    }

    private static LockAndTimestampServices createRawLeaderServices(
            MetricsManager metricsManager,
            LeaderConfig leaderConfig,
            Consumer<Object> env,
            com.google.common.base.Supplier<LockService> lock,
            com.google.common.base.Supplier<TimestampService> time,
            com.google.common.base.Supplier<TimestampManagementService> timeManagement,
            String userAgent) {
        // Create local services, that may or may not end up being registered in an Consumer<Object>.
        LeaderRuntimeConfig defaultRuntime = ImmutableLeaderRuntimeConfig.builder().build();
        LocalPaxosServices localPaxosServices = Leaders.createAndRegisterLocalServices(
                metricsManager,
                env,
                leaderConfig,
                () -> defaultRuntime,
                userAgent);
        LeaderElectionService leader = localPaxosServices.leaderElectionService();
        LockService localLock = ServiceCreator.createInstrumentedService(metricsManager.getRegistry(),
                AwaitingLeadershipProxy.newProxyInstance(LockService.class, lock, leader),
                LockService.class);
        TimestampService localTime = ServiceCreator.createInstrumentedService(metricsManager.getRegistry(),
                AwaitingLeadershipProxy.newProxyInstance(TimestampService.class, time, leader),
                TimestampService.class);
        TimestampManagementService localManagement = ServiceCreator.createInstrumentedService(
                metricsManager.getRegistry(),
                AwaitingLeadershipProxy.newProxyInstance(TimestampManagementService.class,
                        timeManagement,
                        leader),
                TimestampManagementService.class);
        env.accept(localLock);
        env.accept(localTime);
        env.accept(localManagement);

        // Create remote services, that may end up calling our own local services.
        ImmutableServerListConfig serverListConfig = ImmutableServerListConfig.builder()
                .servers(leaderConfig.leaders())
                .sslConfiguration(leaderConfig.sslConfiguration())
                .build();
        LockService remoteLock = new ServiceCreator<>(metricsManager, LockService.class, userAgent)
                .apply(serverListConfig);
        TimestampService remoteTime = new ServiceCreator<>(metricsManager, TimestampService.class, userAgent)
                .apply(serverListConfig);
        TimestampManagementService remoteManagement =
                new ServiceCreator<>(metricsManager, TimestampManagementService.class, userAgent)
                        .apply(serverListConfig);

        if (leaderConfig.leaders().size() == 1) {
            // Attempting to connect to ourself while processing a request can lead to deadlock if incoming request
            // volume is high, as all Jetty threads end up waiting for the timestamp server, and no threads remain to
            // actually handle the timestamp server requests. If we are the only single leader, we can avoid the
            // deadlock entirely; so use PingableLeader's getUUID() to detect this situation and eliminate the redundant
            // call.

            PingableLeader localPingableLeader = localPaxosServices.pingableLeader();
            String localServerId = localPingableLeader.getUUID();
            PingableLeader remotePingableLeader = AtlasDbFeignTargetFactory.createRsProxy(
                    ServiceCreator.createSslSocketFactory(leaderConfig.sslConfiguration()),
                    Iterables.getOnlyElement(leaderConfig.leaders()),
                    PingableLeader.class,
                    userAgent);

            // Determine asynchronously whether the remote services are talking to our local services.
            CompletableFuture<Boolean> useLocalServicesFuture = new CompletableFuture<>();
            runAsync.accept(() -> {
                int logAfter = LOGGING_INTERVAL;
                while (true) {
                    try {
                        String remoteServerId = remotePingableLeader.getUUID();
                        useLocalServicesFuture.complete(localServerId.equals(remoteServerId));
                        return;
                    } catch (ClientErrorException e) {
                        useLocalServicesFuture.complete(false);
                        return;
                    } catch (Throwable e) {
                        if (--logAfter == 0) {
                            log.warn("Failed to read remote timestamp server ID", e);
                            logAfter = LOGGING_INTERVAL;
                        }
                    }
                    Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
                }
            });

            // Create dynamic service proxies, that switch to talking directly to our local services if it turns out our
            // remote services are pointed at them anyway.
            LockService dynamicLockService = LocalOrRemoteProxy.newProxyInstance(
                    LockService.class, localLock, remoteLock, useLocalServicesFuture);
            TimestampService dynamicTimeService = LocalOrRemoteProxy.newProxyInstance(
                    TimestampService.class, localTime, remoteTime, useLocalServicesFuture);
            TimestampManagementService dynamicManagementService = LocalOrRemoteProxy.newProxyInstance(
                    TimestampManagementService.class, localManagement, remoteManagement, useLocalServicesFuture);
            return ImmutableLockAndTimestampServices.builder()
                    .lock(dynamicLockService)
                    .timestamp(dynamicTimeService)
                    .timestampManagement(dynamicManagementService)
                    .timelock(new LegacyTimelockService(dynamicTimeService, dynamicLockService, LOCK_CLIENT))
                    .build();

        } else {
            return ImmutableLockAndTimestampServices.builder()
                    .lock(remoteLock)
                    .timestamp(remoteTime)
                    .timestampManagement(remoteManagement)
                    .timelock(new LegacyTimelockService(remoteTime, remoteLock, LOCK_CLIENT))
                    .build();
        }
    }

    private static LockAndTimestampServices createRawRemoteServices(
            MetricsManager metricsManager, AtlasDbConfig config, String userAgent) {
        LockService lockService = new ServiceCreator<>(metricsManager, LockService.class, userAgent)
                .apply(config.lock().get());
        TimestampService timeService = new ServiceCreator<>(metricsManager, TimestampService.class, userAgent)
                .apply(config.timestamp().get());
        TimestampManagementService timestampManagementService =
                new ServiceCreator<>(metricsManager, TimestampManagementService.class, userAgent)
                        .apply(config.timestamp().get());

        return ImmutableLockAndTimestampServices.builder()
                .lock(lockService)
                .timestamp(timeService)
                .timestampManagement(timestampManagementService)
                .timelock(new LegacyTimelockService(timeService, lockService, LOCK_CLIENT))
                .build();
    }

    private static LockAndTimestampServices createRawEmbeddedServices(
            MetricsManager metricsManager,
            Consumer<Object> env,
            com.google.common.base.Supplier<LockService> lock,
            com.google.common.base.Supplier<TimestampService> time,
            com.google.common.base.Supplier<TimestampManagementService> timeManagement) {
        LockService lockService = ServiceCreator.createInstrumentedService(
                metricsManager.getRegistry(), lock.get(), LockService.class);
        TimestampService timeService = ServiceCreator.createInstrumentedService(
                metricsManager.getRegistry(), time.get(), TimestampService.class);
        TimestampManagementService timestampManagementService = ServiceCreator.createInstrumentedService(
                metricsManager.getRegistry(), timeManagement.get(), TimestampManagementService.class);

        env.accept(lockService);
        env.accept(timeService);
        env.accept(timestampManagementService);

        return ImmutableLockAndTimestampServices.builder()
                .lock(lockService)
                .timestamp(timeService)
                .timestampManagement(timestampManagementService)
                .timelock(new LegacyTimelockService(timeService, lockService, LOCK_CLIENT))
                .build();
    }

    private MultiTableSweepQueueWriter uninitializedTargetedSweeper(
            MetricsManager metricsManager,
            TargetedSweepInstallConfig config,
            Follower follower,
            Supplier<TargetedSweepRuntimeConfig> runtime) {
        if (!config.enableSweepQueueWrites()) {
            return MultiTableSweepQueueWriter.NO_OP;
        }
        return TargetedSweeper.createUninitialized(
                metricsManager,
                Suppliers.compose(TargetedSweepRuntimeConfig::enabled, runtime::get),
                Suppliers.compose(TargetedSweepRuntimeConfig::shards, runtime::get),
                config.conservativeThreads(),
                config.thoroughThreads(),
                ImmutableList.of(follower));
    }

    @Value.Immutable
    @Value.Style(stagedBuilder = false)
    public interface LockAndTimestampServices {
        LockService lock();
        TimestampService timestamp();
        TimestampManagementService timestampManagement();
        TimelockService timelock();
        Optional<TimeLockMigrator> migrator();

        @SuppressWarnings("checkstyle:WhitespaceAround")
        @Value.Default
        default Runnable close() {
            return () -> {};
        }
    }
}
