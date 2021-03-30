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

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.async.initializer.AsyncInitializer;
import com.palantir.async.initializer.Callback;
import com.palantir.async.initializer.LambdaCallback;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.cache.DefaultTimestampCache;
import com.palantir.atlasdb.cache.TimestampCache;
import com.palantir.atlasdb.cleaner.CleanupFollower;
import com.palantir.atlasdb.cleaner.DefaultCleanerBuilder;
import com.palantir.atlasdb.cleaner.Follower;
import com.palantir.atlasdb.cleaner.GlobalClock;
import com.palantir.atlasdb.cleaner.KeyValueServicePuncherStore;
import com.palantir.atlasdb.cleaner.api.Cleaner;
import com.palantir.atlasdb.compact.BackgroundCompactor;
import com.palantir.atlasdb.compact.CompactorConfig;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbRuntimeConfig;
import com.palantir.atlasdb.config.AuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.ImmutableAtlasDbConfig;
import com.palantir.atlasdb.config.ImmutableServerListConfig;
import com.palantir.atlasdb.config.ImmutableTimeLockClientConfig;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.config.RemotingClientConfigs;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.config.ServerListConfigs;
import com.palantir.atlasdb.config.ShouldRunBackgroundSweepSupplier;
import com.palantir.atlasdb.config.SweepConfig;
import com.palantir.atlasdb.config.TimeLockClientConfig;
import com.palantir.atlasdb.config.TimeLockRequestBatcherProviders;
import com.palantir.atlasdb.coordination.CoordinationService;
import com.palantir.atlasdb.debug.ConflictTracer;
import com.palantir.atlasdb.debug.LockDiagnosticComponents;
import com.palantir.atlasdb.debug.LockDiagnosticConjureTimelockService;
import com.palantir.atlasdb.factory.Leaders.LocalPaxosServices;
import com.palantir.atlasdb.factory.startup.ConsistencyCheckRunner;
import com.palantir.atlasdb.factory.startup.TimeLockMigrator;
import com.palantir.atlasdb.factory.timelock.TimestampCorroboratingTimelockService;
import com.palantir.atlasdb.factory.timestamp.FreshTimestampSupplierAdapter;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.http.AtlasDbRemotingConstants;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadata;
import com.palantir.atlasdb.internalschema.TransactionSchemaInstaller;
import com.palantir.atlasdb.internalschema.TransactionSchemaManager;
import com.palantir.atlasdb.internalschema.metrics.MetadataCoordinationServiceMetrics;
import com.palantir.atlasdb.internalschema.persistence.CoordinationServices;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetCompatibility;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.watch.LockWatchEventCacheImpl;
import com.palantir.atlasdb.keyvalue.api.watch.LockWatchManager;
import com.palantir.atlasdb.keyvalue.api.watch.LockWatchManagerImpl;
import com.palantir.atlasdb.keyvalue.api.watch.NoOpLockWatchManager;
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
import com.palantir.atlasdb.schema.TargetedSweepSchema;
import com.palantir.atlasdb.schema.generated.SweepTableFactory;
import com.palantir.atlasdb.schema.generated.TargetedSweepTableFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
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
import com.palantir.atlasdb.sweep.queue.clear.SafeTableClearerKeyValueService;
import com.palantir.atlasdb.sweep.queue.config.TargetedSweepInstallConfig;
import com.palantir.atlasdb.sweep.queue.config.TargetedSweepRuntimeConfig;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.timelock.adjudicate.feedback.TimeLockClientFeedbackService;
import com.palantir.atlasdb.timelock.api.ConjureTimelockService;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.transaction.ImmutableTransactionConfig;
import com.palantir.atlasdb.transaction.TransactionConfig;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.LockWatchingCache;
import com.palantir.atlasdb.transaction.api.NoOpLockWatchingCache;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManager;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManagers;
import com.palantir.atlasdb.transaction.impl.InstrumentedTimelockService;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManagers;
import com.palantir.atlasdb.transaction.impl.TimelockTimestampServiceAdapter;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.impl.consistency.ImmutableTimestampCorroborationConsistencyCheck;
import com.palantir.atlasdb.transaction.impl.metrics.DefaultMetricsFilterEvaluationContext;
import com.palantir.atlasdb.transaction.impl.metrics.MetricsFilterEvaluationContext;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.atlasdb.versions.AtlasDbVersion;
import com.palantir.common.annotation.Output;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.time.Clock;
import com.palantir.conjure.java.api.config.service.ServicesConfigBlock;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.api.config.service.UserAgents;
import com.palantir.conjure.java.api.errors.UnknownRemoteException;
import com.palantir.dialogue.clients.DialogueClients;
import com.palantir.dialogue.clients.DialogueClients.ReloadingFactory;
import com.palantir.leader.PingableLeader;
import com.palantir.leader.proxy.AwaitingLeadershipProxy;
import com.palantir.leader.proxy.LeadershipCoordinator;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockRpcClient;
import com.palantir.lock.LockServerConfigs;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.LockService;
import com.palantir.lock.NamespaceAgnosticLockRpcClient;
import com.palantir.lock.SimpleTimeDuration;
import com.palantir.lock.client.AuthenticatedInternalMultiClientConjureTimelockService;
import com.palantir.lock.client.ImmutableMultiClientRequestBatchers;
import com.palantir.lock.client.InternalMultiClientConjureTimelockService;
import com.palantir.lock.client.LeaderElectionReportingTimelockService;
import com.palantir.lock.client.LeaderTimeCoalescingBatcher;
import com.palantir.lock.client.LeaderTimeGetter;
import com.palantir.lock.client.LegacyLeaderTimeGetter;
import com.palantir.lock.client.LockRefreshingLockService;
import com.palantir.lock.client.NamespacedCoalescingLeaderTimeGetter;
import com.palantir.lock.client.NamespacedConjureLockWatchingService;
import com.palantir.lock.client.ProfilingTimelockService;
import com.palantir.lock.client.ReferenceTrackingWrapper;
import com.palantir.lock.client.RemoteLockServiceAdapter;
import com.palantir.lock.client.RemoteTimelockServiceAdapter;
import com.palantir.lock.client.RequestBatchersFactory;
import com.palantir.lock.client.TimeLockClient;
import com.palantir.lock.client.metrics.TimeLockFeedbackBackgroundTask;
import com.palantir.lock.impl.LegacyTimelockService;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.lock.v2.NamespacedTimelockRpcClient;
import com.palantir.lock.v2.TimelockRpcClient;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.watch.LockWatchEventCache;
import com.palantir.lock.watch.NoOpLockWatchEventCache;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.refreshable.Refreshable;
import com.palantir.timestamp.DelegatingManagedTimestampService;
import com.palantir.timestamp.ManagedTimestampService;
import com.palantir.timestamp.RemoteTimestampManagementAdapter;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampRange;
import com.palantir.timestamp.TimestampService;
import com.palantir.timestamp.TimestampStoreInvalidator;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import com.palantir.util.OptionalResolver;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.ws.rs.ClientErrorException;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Value.Immutable
@Value.Style(stagedBuilder = true)
public abstract class TransactionManagers {
    private static final int LOGGING_INTERVAL = 60;
    private static final Logger log = LoggerFactory.getLogger(TransactionManagers.class);
    public static final LockClient LOCK_CLIENT = LockClient.of("atlas instance");

    abstract AtlasDbConfig config();

    abstract Optional<Refreshable<Optional<AtlasDbRuntimeConfig>>> runtimeConfig();

    /**
     * Use {@link #runtimeConfig} instead.
     */
    abstract Optional<Supplier<Optional<AtlasDbRuntimeConfig>>> runtimeConfigSupplier();

    abstract Set<Schema> schemas();

    @Value.Default
    Consumer<Object> registrar() {
        return resource -> {};
    }

    @Value.Default
    LockServerOptions lockServerOptions() {
        return LockServerConfigs.DEFAULT;
    }

    @Value.Default
    boolean allowHiddenTableAccess() {
        return false;
    }

    @Value.Default
    boolean validateLocksOnReads() {
        return true;
    }

    @Value.Default
    boolean lockImmutableTsOnReadOnlyTransactions() {
        return false;
    }

    @Value.Default
    boolean allSafeForLogging() {
        return false;
    }

    abstract UserAgent userAgent();

    abstract Optional<TimeLockRequestBatcherProviders> timelockRequestBatcherProviders();

    /**
     * Please use {@link #globalTaggedMetricRegistry()} instead. The publishing of metrics to the external metrics
     * registry has ceased; they are all published to the tagged registry instead.
     */
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

    // TODO(fdesouza): Remove this once PDS-95791 is resolved.
    abstract Optional<LockDiagnosticComponents> lockDiagnosticComponents();

    @Value.Default
    LockWatchingCache lockWatchingCache() {
        return NoOpLockWatchingCache.INSTANCE;
    }

    @Value.Default
    MetricsFilterEvaluationContext metricsFilterEvaluationContext() {
        return DefaultMetricsFilterEvaluationContext.createDefault();
    }

    /**
     * If set, a {@link com.palantir.dialogue.clients.DialogueClients.ReloadingFactory} that a
     * {@link com.palantir.atlasdb.transaction.api.TransactionManager} based on this configuration should use.
     * This may be useful for ensuring that connection pools are shared between multiple TransactionManagers in
     * the same JVM.
     * We intend this to be used in exactly one place - do not use this without discussing with the AtlasDB team.
     */
    @Value.Default
    DialogueClients.ReloadingFactory reloadingFactory() {
        return newMinimalDialogueFactory();
    }

    public static ImmutableTransactionManagers.ConfigBuildStage builder() {
        return ImmutableTransactionManagers.builder();
    }

    @SuppressWarnings("immutables:incompat")
    @VisibleForTesting
    static Consumer<Runnable> runAsync = task -> {
        Thread thread = new Thread(task);
        thread.setDaemon(true);
        thread.start();
    };

    @Value.Check
    protected void check() {
        Preconditions.checkState(
                !(runtimeConfigSupplier().isPresent() && runtimeConfig().isPresent()),
                "Cannot provide both Refreshable and Supplier of runtime config");
    }

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
        AtlasDbConfig config = ImmutableAtlasDbConfig.builder()
                .keyValueService(new InMemoryAtlasDbConfig())
                .build();
        return builder()
                .config(config)
                .userAgent(AtlasDbRemotingConstants.DEFAULT_USER_AGENT)
                .globalMetricsRegistry(new MetricRegistry())
                .globalTaggedMetricRegistry(DefaultTaggedMetricRegistry.getDefault())
                .addAllSchemas(schemas)
                .build()
                .serializable();
    }

    @JsonIgnore
    @Value.Derived
    public TransactionManager serializable() {
        List<AutoCloseable> closeables = new ArrayList<>();

        try {
            return serializableInternal(closeables);
        } catch (Throwable throwable) {
            List<String> closeablesClasses = closeables.stream()
                    .map(autoCloseable -> autoCloseable.getClass().toString())
                    .collect(Collectors.toList());

            log.warn(
                    "Exception thrown when creating transaction manager. " + "Closing previously opened resources: {}",
                    SafeArg.of("classes", closeablesClasses.toString()),
                    throwable);

            closeables.forEach(autoCloseable -> {
                try {
                    autoCloseable.close();
                } catch (Exception ex) {
                    log.info(
                            "Error closing {}",
                            SafeArg.of("class", autoCloseable.getClass().toString()),
                            ex);
                }
            });
            throw throwable;
        }
    }

    @SuppressWarnings("MethodLength")
    private TransactionManager serializableInternal(@Output List<AutoCloseable> closeables) {
        MetricsManager metricsManager = setUpMetricsAndGetMetricsManager();

        AtlasDbRuntimeConfigRefreshable runtimeConfigRefreshable =
                initializeCloseable(() -> AtlasDbRuntimeConfigRefreshable.create(this), closeables);

        Refreshable<AtlasDbRuntimeConfig> runtime = runtimeConfigRefreshable.config();

        Optional<TimeLockFeedbackBackgroundTask> timeLockFeedbackBackgroundTask =
                getTimeLockFeedbackBackgroundTask(metricsManager, closeables, config(), runtime);

        FreshTimestampSupplierAdapter adapter = new FreshTimestampSupplierAdapter();
        ServiceDiscoveringAtlasSupplier atlasFactory = new ServiceDiscoveringAtlasSupplier(
                metricsManager,
                config().keyValueService(),
                runtime.map(AtlasDbRuntimeConfig::keyValueService),
                config().leader(),
                config().namespace(),
                Optional.empty(),
                config().initializeAsync(),
                adapter);
        KeyValueServiceConfig mergedKeyValueServiceConfig = atlasFactory.getMergedKeyValueServiceConfig();

        LockRequest.setDefaultLockTimeout(
                SimpleTimeDuration.of(config().getDefaultLockTimeoutSeconds(), TimeUnit.SECONDS));

        Supplier<ManagedTimestampService> managedTimestampSupplier = atlasFactory::getManagedTimestampService;

        LockAndTimestampServices lockAndTimestampServices = createLockAndTimestampServices(
                metricsManager,
                config(),
                runtime,
                registrar(),
                () -> LockServiceImpl.create(lockServerOptions()),
                managedTimestampSupplier,
                atlasFactory.getTimestampStoreInvalidator(),
                userAgent(),
                lockDiagnosticComponents(),
                reloadingFactory(),
                timeLockFeedbackBackgroundTask,
                timelockRequestBatcherProviders());
        adapter.setTimestampService(lockAndTimestampServices.managedTimestampService());

        KvsProfilingLogger.setSlowLogThresholdMillis(config().getKvsSlowLogThresholdMillis());

        Refreshable<SweepConfig> sweepConfig = runtime.map(AtlasDbRuntimeConfig::sweep);

        KeyValueService keyValueService = initializeCloseable(
                () -> {
                    KeyValueService kvs = atlasFactory.getKeyValueService();
                    kvs = ProfilingKeyValueService.create(kvs);
                    kvs = new SafeTableClearerKeyValueService(
                            lockAndTimestampServices.timelock()::getImmutableTimestamp, kvs);

                    // Even if sweep queue writes are enabled, unless targeted sweep is enabled we generally still want
                    // to
                    // at least retain the option to perform background sweep, which requires updating the priority
                    // table.
                    if (!targetedSweepIsFullyEnabled(config(), runtime)) {
                        kvs = SweepStatsKeyValueService.create(
                                kvs,
                                new TimelockTimestampServiceAdapter(lockAndTimestampServices.timelock()),
                                sweepConfig.map(SweepConfig::writeThreshold),
                                sweepConfig.map(SweepConfig::writeSizeThreshold),
                                () -> true);
                    }

                    kvs = TracingKeyValueService.create(kvs);
                    kvs = AtlasDbMetrics.instrumentTimed(
                            metricsManager.getRegistry(),
                            KeyValueService.class,
                            kvs,
                            MetricRegistry.name(KeyValueService.class));
                    return ValidatingQueryRewritingKeyValueService.create(kvs);
                },
                closeables);

        TransactionManagersInitializer initializer = TransactionManagersInitializer.createInitialTables(
                keyValueService, schemas(), config().initializeAsync(), allSafeForLogging());
        PersistentLockService persistentLockService =
                createAndRegisterPersistentLockService(keyValueService, registrar(), config().initializeAsync());

        TransactionComponents components = createTransactionComponents(
                closeables, metricsManager, lockAndTimestampServices, keyValueService, runtime);
        TransactionService transactionService = components.transactionService();
        ConflictDetectionManager conflictManager = ConflictDetectionManagers.create(keyValueService);
        SweepStrategyManager sweepStrategyManager = SweepStrategyManagers.createDefault(keyValueService);

        CleanupFollower follower = CleanupFollower.create(schemas());

        Cleaner cleaner = initializeCloseable(
                () -> new DefaultCleanerBuilder(
                                keyValueService,
                                lockAndTimestampServices.timelock(),
                                ImmutableList.of(follower),
                                transactionService,
                                metricsManager)
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
                () -> uninitializedTargetedSweeper(
                        metricsManager,
                        config().targetedSweep(),
                        follower,
                        runtime.map(AtlasDbRuntimeConfig::targetedSweep)),
                closeables);

        Callback<TransactionManager> callbacks = new Callback.CallChain<>(
                timelockConsistencyCheckCallback(config(), runtime.get(), lockAndTimestampServices),
                targetedSweep.singleAttemptCallback(),
                asyncInitializationCallback(),
                createClearsTable());

        Supplier<TransactionConfig> transactionConfigSupplier =
                runtime.map(AtlasDbRuntimeConfig::transaction).map(this::withConsolidatedGrabImmutableTsLockFlag);

        TimestampCache timestampCache = config().timestampCache()
                .orElseGet(() -> new DefaultTimestampCache(
                        metricsManager.getRegistry(), () -> runtime.get().getTimestampCacheSize()));

        ConflictTracer conflictTracer = lockDiagnosticComponents()
                .map(LockDiagnosticComponents::clientLockDiagnosticCollector)
                .<ConflictTracer>map(Function.identity())
                .orElse(ConflictTracer.NO_OP);

        TransactionManager transactionManager = initializeCloseable(
                () -> SerializableTransactionManager.createInstrumented(
                        metricsManager,
                        keyValueService,
                        lockAndTimestampServices.timelock(),
                        lockAndTimestampServices.lockWatcher(),
                        lockAndTimestampServices.eventCache(),
                        lockAndTimestampServices.managedTimestampService(),
                        lockAndTimestampServices.lock(),
                        transactionService,
                        () -> AtlasDbConstraintCheckingMode.FULL_CONSTRAINT_CHECKING_THROWS_EXCEPTIONS,
                        conflictManager,
                        sweepStrategyManager,
                        cleaner,
                        () -> areTransactionManagerInitializationPrerequisitesSatisfied(
                                initializer, lockAndTimestampServices),
                        allowHiddenTableAccess(),
                        mergedKeyValueServiceConfig.concurrentGetRangesThreadPoolSize(),
                        mergedKeyValueServiceConfig.defaultGetRangesConcurrency(),
                        config().initializeAsync(),
                        timestampCache,
                        targetedSweep,
                        callbacks,
                        validateLocksOnReads(),
                        transactionConfigSupplier,
                        conflictTracer,
                        metricsFilterEvaluationContext()),
                closeables);

        transactionManager.registerClosingCallback(runtimeConfigRefreshable::close);

        timeLockFeedbackBackgroundTask.ifPresent(task -> transactionManager.registerClosingCallback(task::close));

        lockAndTimestampServices.resources().forEach(transactionManager::registerClosingCallback);
        transactionManager.registerClosingCallback(transactionService::close);
        components
                .schemaInstaller()
                .ifPresent(installer -> transactionManager.registerClosingCallback(installer::close));
        transactionManager.registerClosingCallback(targetedSweep::close);

        PersistentLockManager persistentLockManager = initializeCloseable(
                () -> new PersistentLockManager(
                        metricsManager, persistentLockService, config().getSweepPersistentLockWaitMillis()),
                closeables);
        transactionManager.registerClosingCallback(persistentLockManager::close);

        initializeCloseable(
                () -> initializeSweepEndpointAndBackgroundProcess(
                        metricsManager,
                        config(),
                        runtime,
                        registrar(),
                        keyValueService,
                        transactionService,
                        sweepStrategyManager,
                        follower,
                        transactionManager,
                        persistentLockManager,
                        runBackgroundSweepProcess()),
                closeables);
        initializeCloseable(
                initializeCompactBackgroundProcess(
                        metricsManager,
                        lockAndTimestampServices,
                        keyValueService,
                        transactionManager,
                        runtime.map(AtlasDbRuntimeConfig::compact)),
                closeables);

        log.info("Successfully created, and now returning a transaction manager: this may not be fully initialised.");
        return transactionManager;
    }

    private MetricsManager setUpMetricsAndGetMetricsManager() {
        MetricRegistry internalAtlasDbMetrics = new MetricRegistry();
        TaggedMetricRegistry internalTaggedAtlasDbMetrics = new DefaultTaggedMetricRegistry();
        MetricsManager metricsManager = MetricsManagers.of(
                internalAtlasDbMetrics,
                internalTaggedAtlasDbMetrics,
                runtimeConfig()
                        .map(runtimeConfigRefreshable -> runtimeConfigRefreshable.map(maybeRuntime -> maybeRuntime
                                .map(AtlasDbRuntimeConfig::enableMetricFiltering)
                                .orElse(true)))
                        .orElseGet(() -> Refreshable.only(true)));
        globalTaggedMetricRegistry()
                .addMetrics(
                        AtlasDbMetricNames.LIBRARY_ORIGIN_TAG,
                        AtlasDbMetricNames.LIBRARY_ORIGIN_VALUE,
                        metricsManager.getPublishableMetrics());
        return metricsManager;
    }

    private Optional<TimeLockFeedbackBackgroundTask> getTimeLockFeedbackBackgroundTask(
            MetricsManager metricsManager,
            @Output List<AutoCloseable> closeables,
            AtlasDbConfig config,
            Refreshable<AtlasDbRuntimeConfig> runtimeConfig) {
        if (isUsingTimeLock(config, runtimeConfig.current())) {
            Refreshable<List<TimeLockClientFeedbackService>> refreshableTimeLockClientFeedbackServices =
                    getTimeLockClientFeedbackServices(config, runtimeConfig, userAgent(), reloadingFactory());
            return Optional.of(initializeCloseable(
                    () -> TimeLockFeedbackBackgroundTask.create(
                            metricsManager.getTaggedRegistry(),
                            AtlasDbVersion::readVersion,
                            serviceName(),
                            refreshableTimeLockClientFeedbackServices,
                            namespace()),
                    closeables));
        }
        return Optional.empty();
    }

    @VisibleForTesting
    static Refreshable<List<TimeLockClientFeedbackService>> getTimeLockClientFeedbackServices(
            AtlasDbConfig config,
            Refreshable<AtlasDbRuntimeConfig> runtimeConfig,
            UserAgent userAgent,
            DialogueClients.ReloadingFactory reloadingFactory) {
        Refreshable<ServerListConfig> serverListConfigSupplier =
                getServerListConfigSupplierForTimeLock(config, runtimeConfig);

        BroadcastDialogueClientFactory broadcastDialogueClientFactory = BroadcastDialogueClientFactory.create(
                reloadingFactory,
                serverListConfigSupplier,
                userAgent,
                AuxiliaryRemotingParameters.builder()
                        .shouldRetry(true)
                        .userAgent(userAgent)
                        .shouldLimitPayload(true)
                        .build());

        return broadcastDialogueClientFactory.getSingleNodeProxies(TimeLockClientFeedbackService.class, true);
    }

    private static DialogueClients.ReloadingFactory newMinimalDialogueFactory() {
        return DialogueClients.create(
                        Refreshable.only(ServicesConfigBlock.builder().build()))
                .withBlockingExecutor(PTExecutors.newCachedThreadPool("atlas-dialogue-blocking"));
    }

    abstract Optional<String> serviceIdentifierOverride();

    @VisibleForTesting
    @Value.Derived
    String serviceName() {
        return serviceIdentifierOverride().orElseGet(this::namespace);
    }

    @Value.Derived
    String namespace() {
        return Stream.of(
                        config().namespace(),
                        config().timelock().flatMap(TimeLockClientConfig::client),
                        config().keyValueService().namespace())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst()
                .orElse("UNKNOWN");
    }

    private static Callback<TransactionManager> createClearsTable() {
        TableReference clearsTableRef =
                TargetedSweepTableFactory.of().getTableClearsTable(null).getTableRef();
        byte[] clearsTableMetadata = TargetedSweepSchema.INSTANCE
                .getLatestSchema()
                .getAllTablesAndIndexMetadata()
                .get(clearsTableRef)
                .persistToBytes();
        return LambdaCallback.of(tm -> tm.getKeyValueService().createTable(clearsTableRef, clearsTableMetadata));
    }

    /**
     * If we decide to move a service to use thorough sweep; we need to make sure that background sweep won't cause any
     * trouble by deleting large number of empty values at once - causing Cassandra OOMs.
     *
     * lockImmutableTsOnReadOnlyTransaction flag is used to decide on disabling background sweep, as this flag is used
     * as an intermediate step for migrating to thorough sweep.
     *
     * Separately, users may disable the background sweep process entirely in config, which we respect.
     */
    private boolean runBackgroundSweepProcess() {
        return config().runBackgroundSweepProcess() && !lockImmutableTsOnReadOnlyTransactions();
    }

    @VisibleForTesting
    TransactionConfig withConsolidatedGrabImmutableTsLockFlag(TransactionConfig transactionConfig) {
        return ImmutableTransactionConfig.copyOf(transactionConfig)
                .withLockImmutableTsOnReadOnlyTransactions(lockImmutableTsOnReadOnlyTransactions()
                        || transactionConfig.lockImmutableTsOnReadOnlyTransactions());
    }

    private static boolean targetedSweepIsFullyEnabled(
            AtlasDbConfig installConfig, Supplier<AtlasDbRuntimeConfig> runtime) {
        return installConfig.targetedSweep().enableSweepQueueWrites()
                && runtime.get().targetedSweep().enabled();
    }

    private TransactionComponents createTransactionComponents(
            @Output List<AutoCloseable> closeables,
            MetricsManager metricsManager,
            LockAndTimestampServices lockAndTimestampServices,
            KeyValueService keyValueService,
            Supplier<AtlasDbRuntimeConfig> runtimeConfigSupplier) {
        CoordinationService<InternalSchemaMetadata> coordinationService =
                getSchemaMetadataCoordinationService(metricsManager, lockAndTimestampServices, keyValueService);
        TransactionSchemaManager transactionSchemaManager = new TransactionSchemaManager(coordinationService);

        TransactionService transactionService = initializeCloseable(
                () -> AtlasDbMetrics.instrumentTimed(
                        metricsManager.getRegistry(),
                        TransactionService.class,
                        TransactionServices.createTransactionService(keyValueService, transactionSchemaManager)),
                closeables);
        Optional<TransactionSchemaInstaller> schemaInstaller = getTransactionSchemaInstallerIfSupported(
                closeables, keyValueService, runtimeConfigSupplier, transactionSchemaManager);
        return ImmutableTransactionComponents.builder()
                .transactionService(transactionService)
                .schemaInstaller(schemaInstaller)
                .build();
    }

    private static Optional<TransactionSchemaInstaller> getTransactionSchemaInstallerIfSupported(
            @Output List<AutoCloseable> closeables,
            KeyValueService keyValueService,
            Supplier<AtlasDbRuntimeConfig> runtimeConfigSupplier,
            TransactionSchemaManager transactionSchemaManager) {
        if (keyValueService.getCheckAndSetCompatibility() == CheckAndSetCompatibility.SUPPORTED_DETAIL_ON_FAILURE) {
            return Optional.of(
                    initializeTransactionSchemaInstaller(closeables, runtimeConfigSupplier, transactionSchemaManager));
        }
        runtimeConfigSupplier
                .get()
                .internalSchema()
                .targetTransactionsSchemaVersion()
                .filter(version -> version != TransactionConstants.DIRECT_ENCODING_TRANSACTIONS_SCHEMA_VERSION)
                .ifPresent(version -> log.warn(
                        "This service seems like it has been configured to use transaction schema version {},"
                                + " which isn't supported as your KVS doesn't support details on CAS failures"
                                + " (typically Postgres or Oracle). We will remain with transactions1.",
                        SafeArg.of("configuredTransactionSchemaVersion", version)));
        return Optional.empty();
    }

    private static TransactionSchemaInstaller initializeTransactionSchemaInstaller(
            @Output List<AutoCloseable> closeables,
            Supplier<AtlasDbRuntimeConfig> runtimeConfigSupplier,
            TransactionSchemaManager transactionSchemaManager) {
        return initializeCloseable(
                () -> TransactionSchemaInstaller.createStarted(
                        transactionSchemaManager,
                        () -> runtimeConfigSupplier.get().internalSchema().targetTransactionsSchemaVersion()),
                closeables);
    }

    private CoordinationService<InternalSchemaMetadata> getSchemaMetadataCoordinationService(
            MetricsManager metricsManager,
            LockAndTimestampServices lockAndTimestampServices,
            KeyValueService keyValueService) {
        CoordinationService<InternalSchemaMetadata> metadataCoordinationService = CoordinationServices.createDefault(
                keyValueService,
                lockAndTimestampServices.managedTimestampService(),
                metricsManager,
                config().initializeAsync());
        MetadataCoordinationServiceMetrics.registerMetrics(
                metricsManager, metadataCoordinationService, lockAndTimestampServices.managedTimestampService());
        return metadataCoordinationService;
    }

    private static Optional<BackgroundCompactor> initializeCompactBackgroundProcess(
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

        backgroundCompactorOptional.ifPresent(
                backgroundCompactor -> transactionManager.registerClosingCallback(backgroundCompactor::close));

        return backgroundCompactorOptional;
    }

    private static <T extends AutoCloseable> T initializeCloseable(
            Supplier<T> closeableSupplier, @Output List<AutoCloseable> closeables) {
        T ret = closeableSupplier.get();
        closeables.add(ret);
        return ret;
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private static <T extends AutoCloseable> Optional<T> initializeCloseable(
            Optional<T> closeableOptional, @Output List<AutoCloseable> closeables) {
        closeableOptional.ifPresent(closeables::add);
        return closeableOptional;
    }

    private static boolean areTransactionManagerInitializationPrerequisitesSatisfied(
            AsyncInitializer initializer, LockAndTimestampServices lockAndTimestampServices) {
        return initializer.isInitialized() && timeLockMigrationCompleteIfNeeded(lockAndTimestampServices);
    }

    @VisibleForTesting
    static boolean timeLockMigrationCompleteIfNeeded(LockAndTimestampServices lockAndTimestampServices) {
        return lockAndTimestampServices
                .migrator()
                .map(AsyncInitializer::isInitialized)
                .orElse(true);
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
            PersistentLockManager persistentLockManager,
            boolean runInBackground) {
        CellsSweeper cellsSweeper =
                new CellsSweeper(transactionManager, kvs, persistentLockManager, ImmutableList.of(follower));

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

        boolean sweepQueueWritesEnabled = config.targetedSweep().enableSweepQueueWrites();
        BackgroundSweeperImpl backgroundSweeper = BackgroundSweeperImpl.create(
                metricsManager,
                sweepBatchConfigSource,
                new ShouldRunBackgroundSweepSupplier(
                        () -> runtimeConfigSupplier.get().sweep(), sweepQueueWritesEnabled)::getAsBoolean,
                () -> runtimeConfigSupplier.get().sweep().sweepThreads(),
                () -> runtimeConfigSupplier.get().sweep().pauseMillis(),
                () -> runtimeConfigSupplier.get().sweep().sweepPriorityOverrides(),
                specificTableSweeper);

        transactionManager.registerClosingCallback(backgroundSweeper::shutdown);

        if (runInBackground) {
            backgroundSweeper.runInBackground();
        }

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
                .candidateBatchSize(
                        sweepConfig.candidateBatchHint().orElse(AtlasDbConstants.DEFAULT_SWEEP_CANDIDATE_BATCH_HINT))
                .deleteBatchSize(sweepConfig.deleteBatchHint())
                .build();
    }

    private static PersistentLockService createAndRegisterPersistentLockService(
            KeyValueService kvs, Consumer<Object> env, boolean initializeAsync) {
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
        // Only do the consistency check if we're using TimeLock.
        // This avoids a bootstrapping problem with leader-block services without async initialisation,
        // where you need a working timestamp service to check consistency, you need to check consistency
        // before you can return a TM, you need to return a TM to listen on ports, and you need to listen on
        // ports in order to get a working timestamp service.
        if (isUsingTimeLock(atlasDbConfig, initialRuntimeConfig)) {
            ToLongFunction<TransactionManager> conservativeBoundSupplier = txnManager -> {
                Clock clock = GlobalClock.create(txnManager.getTimelockService());
                return KeyValueServicePuncherStore.get(txnManager.getKeyValueService(), clock.getTimeMillis());
            };
            return ConsistencyCheckRunner.create(ImmutableTimestampCorroborationConsistencyCheck.builder()
                    .conservativeBound(conservativeBoundSupplier)
                    .freshTimestampSource(
                            unused -> lockAndTimestampServices.timelock().getFreshTimestamp())
                    .build());
        }
        return Callback.noOp();
    }

    private static boolean isUsingTimeLock(AtlasDbConfig atlasDbConfig, AtlasDbRuntimeConfig runtimeConfig) {
        return atlasDbConfig.timelock().isPresent()
                || runtimeConfig.timelockRuntime().isPresent();
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
            Refreshable<AtlasDbRuntimeConfig> runtimeConfigSupplier,
            Consumer<Object> env,
            Supplier<LockService> lock,
            Supplier<ManagedTimestampService> time,
            TimestampStoreInvalidator invalidator,
            String userAgent) {
        LockAndTimestampServices lockAndTimestampServices = createRawInstrumentedServices(
                metricsManager,
                config,
                runtimeConfigSupplier,
                env,
                lock,
                time,
                invalidator,
                UserAgents.tryParse(userAgent),
                Optional.empty(),
                newMinimalDialogueFactory(),
                Optional.empty(),
                Optional.empty());
        TimeLockClient timeLockClient = TimeLockClient.withSynchronousUnlocker(lockAndTimestampServices.timelock());
        return ImmutableLockAndTimestampServices.builder()
                .from(lockAndTimestampServices)
                .timelock(timeLockClient)
                .lock(LockRefreshingLockService.create(lockAndTimestampServices.lock()))
                .addResources(timeLockClient::close)
                .build();
    }

    @VisibleForTesting
    static LockAndTimestampServices createLockAndTimestampServices(
            MetricsManager metricsManager,
            AtlasDbConfig config,
            Refreshable<AtlasDbRuntimeConfig> runtimeConfig,
            Consumer<Object> env,
            Supplier<LockService> lock,
            Supplier<ManagedTimestampService> time,
            TimestampStoreInvalidator invalidator,
            UserAgent userAgent,
            Optional<LockDiagnosticComponents> lockDiagnosticComponents,
            ReloadingFactory reloadingFactory,
            Optional<TimeLockFeedbackBackgroundTask> timeLockFeedbackBackgroundTask,
            Optional<TimeLockRequestBatcherProviders> timelockRequestBatcherProviders) {
        LockAndTimestampServices lockAndTimestampServices = createRawInstrumentedServices(
                metricsManager,
                config,
                runtimeConfig,
                env,
                lock,
                time,
                invalidator,
                userAgent,
                lockDiagnosticComponents,
                reloadingFactory,
                timeLockFeedbackBackgroundTask,
                timelockRequestBatcherProviders);
        return withMetrics(
                metricsManager, withCorroboratingTimestampService(withRefreshingLockService(lockAndTimestampServices)));
    }

    private static LockAndTimestampServices withCorroboratingTimestampService(
            LockAndTimestampServices lockAndTimestampServices) {
        TimelockService timelockService =
                TimestampCorroboratingTimelockService.create(lockAndTimestampServices.timelock());
        TimestampService corroboratingTimestampService = new TimelockTimestampServiceAdapter(timelockService);

        return ImmutableLockAndTimestampServices.builder()
                .from(lockAndTimestampServices)
                .timelock(timelockService)
                .timestamp(corroboratingTimestampService)
                .build();
    }

    private static LockAndTimestampServices withRefreshingLockService(
            LockAndTimestampServices lockAndTimestampServices) {
        TimeLockClient timeLockClient = TimeLockClient.createDefault(lockAndTimestampServices.timelock());
        ProfilingTimelockService profilingService = ProfilingTimelockService.create(timeLockClient);
        return ImmutableLockAndTimestampServices.builder()
                .from(lockAndTimestampServices)
                .timestamp(new TimelockTimestampServiceAdapter(profilingService))
                .timelock(profilingService)
                .lock(LockRefreshingLockService.create(lockAndTimestampServices.lock()))
                .addResources(timeLockClient::close)
                .addResources(profilingService::close)
                .build();
    }

    private static LockAndTimestampServices withMetrics(
            MetricsManager metricsManager, LockAndTimestampServices lockAndTimestampServices) {
        TimelockService timelockServiceWithBatching = lockAndTimestampServices.timelock();
        TimelockService instrumentedTimelockService =
                InstrumentedTimelockService.create(timelockServiceWithBatching, metricsManager);

        return ImmutableLockAndTimestampServices.builder()
                .from(lockAndTimestampServices)
                .timestamp(new TimelockTimestampServiceAdapter(instrumentedTimelockService))
                .timelock(instrumentedTimelockService)
                .build();
    }

    private static LockAndTimestampServices createRawInstrumentedServices(
            MetricsManager metricsManager,
            AtlasDbConfig config,
            Refreshable<AtlasDbRuntimeConfig> runtimeConfig,
            Consumer<Object> env,
            Supplier<LockService> lock,
            Supplier<ManagedTimestampService> time,
            TimestampStoreInvalidator invalidator,
            UserAgent userAgent,
            Optional<LockDiagnosticComponents> lockDiagnosticComponents,
            ReloadingFactory reloadingFactory,
            Optional<TimeLockFeedbackBackgroundTask> timeLockFeedbackBackgroundTask,
            Optional<TimeLockRequestBatcherProviders> timelockRequestBatcherProviders) {
        AtlasDbRuntimeConfig initialRuntimeConfig = runtimeConfig.get();
        assertNoSpuriousTimeLockBlockInRuntimeConfig(config, initialRuntimeConfig);
        if (config.leader().isPresent()) {
            return createRawLeaderServices(metricsManager, config.leader().get(), env, lock, time, userAgent);
        } else if (config.timestamp().isPresent() && config.lock().isPresent()) {
            return createRawRemoteServices(metricsManager, config, runtimeConfig, userAgent);
        } else if (isUsingTimeLock(config, initialRuntimeConfig)) {
            return createRawServicesFromTimeLock(
                    metricsManager,
                    config,
                    runtimeConfig,
                    invalidator,
                    userAgent,
                    lockDiagnosticComponents,
                    reloadingFactory,
                    timeLockFeedbackBackgroundTask,
                    timelockRequestBatcherProviders);
        } else {
            return createRawEmbeddedServices(metricsManager, env, lock, time);
        }
    }

    private static void assertNoSpuriousTimeLockBlockInRuntimeConfig(
            AtlasDbConfig config, AtlasDbRuntimeConfig initialRuntimeConfig) {
        // Note: The other direction (timelock install config without a runtime block) should be maintained for
        // backwards compatibility.
        if (remoteTimestampAndLockOrLeaderBlocksPresent(config)
                && initialRuntimeConfig.timelockRuntime().isPresent()) {
            throw new SafeIllegalStateException("Found a service configured not to use timelock, with a timelock"
                    + " block in the runtime config! This is unexpected. If you wish to use non-timelock services,"
                    + " please remove the timelock block from the runtime config; if you wish to use timelock,"
                    + " please remove the leader, remote timestamp or remote lock configuration blocks.");
        }
    }

    private static boolean remoteTimestampAndLockOrLeaderBlocksPresent(AtlasDbConfig config) {
        return (config.timestamp().isPresent() && config.lock().isPresent())
                || config.leader().isPresent();
    }

    private static LockAndTimestampServices createRawServicesFromTimeLock(
            MetricsManager metricsManager,
            AtlasDbConfig config,
            Refreshable<AtlasDbRuntimeConfig> runtimeConfig,
            TimestampStoreInvalidator invalidator,
            UserAgent userAgent,
            Optional<LockDiagnosticComponents> lockDiagnosticComponents,
            ReloadingFactory reloadingFactory,
            Optional<TimeLockFeedbackBackgroundTask> timeLockFeedbackBackgroundTask,
            Optional<TimeLockRequestBatcherProviders> timelockRequestBatcherProviders) {
        Refreshable<ServerListConfig> serverListConfigSupplier =
                getServerListConfigSupplierForTimeLock(config, runtimeConfig);

        String timelockNamespace =
                OptionalResolver.resolve(config.timelock().flatMap(TimeLockClientConfig::client), config.namespace());
        LockAndTimestampServices lockAndTimestampServices = getLockAndTimestampServices(
                metricsManager,
                serverListConfigSupplier,
                userAgent,
                timelockNamespace,
                lockDiagnosticComponents,
                reloadingFactory,
                timeLockFeedbackBackgroundTask,
                timelockRequestBatcherProviders);

        TimeLockMigrator migrator = TimeLockMigrator.create(
                lockAndTimestampServices.managedTimestampService(), invalidator, config.initializeAsync());
        migrator.migrate(); // This can proceed async if config.initializeAsync() was set

        return ImmutableLockAndTimestampServices.copyOf(lockAndTimestampServices)
                .withMigrator(migrator);
    }

    private static Refreshable<ServerListConfig> getServerListConfigSupplierForTimeLock(
            AtlasDbConfig config, Refreshable<AtlasDbRuntimeConfig> runtimeConfigSupplier) {
        Preconditions.checkState(
                !remoteTimestampAndLockOrLeaderBlocksPresent(config),
                "Cannot create raw services from timelock with another source of timestamps/locks configured!");
        TimeLockClientConfig clientConfig = config.timelock()
                .orElseGet(() -> ImmutableTimeLockClientConfig.builder().build());
        return ServerListConfigs.parseInstallAndRuntimeConfigs(
                clientConfig, runtimeConfigSupplier.map(AtlasDbRuntimeConfig::timelockRuntime));
    }

    private static LockAndTimestampServices getLockAndTimestampServices(
            MetricsManager metricsManager,
            Refreshable<ServerListConfig> timelockServerListConfig,
            UserAgent userAgent,
            String timelockNamespace,
            Optional<LockDiagnosticComponents> lockDiagnosticComponents,
            ReloadingFactory reloadingFactory,
            Optional<TimeLockFeedbackBackgroundTask> timeLockFeedbackBackgroundTask,
            Optional<TimeLockRequestBatcherProviders> timelockRequestBatcherProviders) {
        AtlasDbDialogueServiceProvider serviceProvider = AtlasDbDialogueServiceProvider.create(
                timelockServerListConfig, reloadingFactory, userAgent, metricsManager.getTaggedRegistry());

        LockRpcClient lockRpcClient = serviceProvider.getLockRpcClient();
        LockService lockService = AtlasDbMetrics.instrumentTimed(
                metricsManager.getRegistry(),
                LockService.class,
                RemoteLockServiceAdapter.create(lockRpcClient, timelockNamespace));

        ConjureTimelockService conjureTimelockService = serviceProvider.getConjureTimelockService();
        TimelockRpcClient timelockClient = serviceProvider.getTimelockRpcClient();

        // TODO(fdesouza): Remove this once PDS-95791 is resolved.
        ConjureTimelockService withDiagnosticsConjureTimelockService = lockDiagnosticComponents
                .<ConjureTimelockService>map(components -> new LockDiagnosticConjureTimelockService(
                        conjureTimelockService,
                        components.clientLockDiagnosticCollector(),
                        components.localLockTracker()))
                .orElse(conjureTimelockService);

        NamespacedTimelockRpcClient namespacedTimelockRpcClient =
                new NamespacedTimelockRpcClient(timelockClient, timelockNamespace);
        LeaderElectionReportingTimelockService namespacedConjureTimelockService =
                LeaderElectionReportingTimelockService.create(withDiagnosticsConjureTimelockService, timelockNamespace);

        timeLockFeedbackBackgroundTask.ifPresent(
                task -> task.registerLeaderElectionStatistics(namespacedConjureTimelockService));

        LockWatchEventCache lockWatchEventCache = LockWatchEventCacheImpl.create(metricsManager);
        NamespacedConjureLockWatchingService lockWatchingService = new NamespacedConjureLockWatchingService(
                serviceProvider.getConjureLockWatchingService(), timelockNamespace);
        LockWatchManagerImpl lockWatchManager = new LockWatchManagerImpl(lockWatchEventCache, lockWatchingService);

        Supplier<InternalMultiClientConjureTimelockService> multiClientTimelockServiceSupplier =
                getMultiClientTimelockServiceSupplier(serviceProvider);

        RemoteTimelockServiceAdapter remoteTimelockServiceAdapter = RemoteTimelockServiceAdapter.create(
                namespacedTimelockRpcClient,
                namespacedConjureTimelockService,
                getLeaderTimeGetter(
                        timelockNamespace,
                        timelockRequestBatcherProviders,
                        namespacedConjureTimelockService,
                        multiClientTimelockServiceSupplier),
                getRequestBatchersFactory(
                        timelockNamespace,
                        timelockRequestBatcherProviders,
                        lockWatchEventCache,
                        multiClientTimelockServiceSupplier));
        TimestampManagementService timestampManagementService = new RemoteTimestampManagementAdapter(
                serviceProvider.getTimestampManagementRpcClient(), timelockNamespace);

        return ImmutableLockAndTimestampServices.builder()
                .lock(lockService)
                .timestamp(new TimelockTimestampServiceAdapter(remoteTimelockServiceAdapter))
                .timestampManagement(timestampManagementService)
                .timelock(remoteTimelockServiceAdapter)
                .lockWatcher(lockWatchManager)
                .eventCache(lockWatchEventCache)
                .addResources(remoteTimelockServiceAdapter::close)
                .addResources(lockWatchManager::close)
                .build();
    }

    private static RequestBatchersFactory getRequestBatchersFactory(
            String namespace,
            Optional<TimeLockRequestBatcherProviders> timelockRequestBatcherProviders,
            LockWatchEventCache lockWatchEventCache,
            Supplier<InternalMultiClientConjureTimelockService> multiClientTimelockServiceSupplier) {
        return RequestBatchersFactory.create(
                lockWatchEventCache,
                Namespace.of(namespace),
                timelockRequestBatcherProviders.map(batcherProviders -> ImmutableMultiClientRequestBatchers.of(
                        batcherProviders.commitTimestamps().getBatcher(multiClientTimelockServiceSupplier),
                        batcherProviders.startTransactions().getBatcher(multiClientTimelockServiceSupplier))));
    }

    private static LeaderTimeGetter getLeaderTimeGetter(
            String timelockNamespace,
            Optional<TimeLockRequestBatcherProviders> timelockRequestBatcherProviders,
            LeaderElectionReportingTimelockService namespacedConjureTimelockService,
            Supplier<InternalMultiClientConjureTimelockService> multiClientTimelockServiceSupplier) {

        if (!timelockRequestBatcherProviders.isPresent()) {
            return new LegacyLeaderTimeGetter(namespacedConjureTimelockService);
        }

        ReferenceTrackingWrapper<LeaderTimeCoalescingBatcher> batcher =
                timelockRequestBatcherProviders.get().leaderTime().getBatcher(multiClientTimelockServiceSupplier);
        return new NamespacedCoalescingLeaderTimeGetter(timelockNamespace, batcher);
    }

    private static Supplier<InternalMultiClientConjureTimelockService> getMultiClientTimelockServiceSupplier(
            AtlasDbDialogueServiceProvider serviceProvider) {
        return Suppliers.memoize(() -> new AuthenticatedInternalMultiClientConjureTimelockService(
                serviceProvider.getMultiClientConjureTimelockService()));
    }

    private static LockAndTimestampServices createRawLeaderServices(
            MetricsManager metricsManager,
            LeaderConfig leaderConfig,
            Consumer<Object> env,
            Supplier<LockService> lock,
            Supplier<ManagedTimestampService> time,
            UserAgent userAgent) {
        // Create local services, that may or may not end up being registered in an Consumer<Object>.
        LocalPaxosServices localPaxosServices =
                Leaders.createAndRegisterLocalServices(metricsManager, env, leaderConfig, userAgent);

        LeadershipCoordinator leadershipCoordinator = localPaxosServices.leadershipCoordinator();
        LockService localLock =
                AwaitingLeadershipProxy.newProxyInstance(LockService.class, lock, leadershipCoordinator);

        ManagedTimestampService managedTimestampProxy =
                AwaitingLeadershipProxy.newProxyInstance(ManagedTimestampService.class, time, leadershipCoordinator);

        // These facades are necessary because of the semantics of the JAX-RS algorithm (in particular, accepting
        // just the managed timestamp service will *not* work).
        TimestampService localTime = getTimestampFacade(managedTimestampProxy);
        TimestampManagementService localManagement = getTimestampManagementFacade(managedTimestampProxy);

        env.accept(localLock);
        env.accept(localTime);
        env.accept(localManagement);

        // Create remote services, that may end up calling our own local services.
        ImmutableServerListConfig serverListConfig = ImmutableServerListConfig.builder()
                .servers(leaderConfig.leaders())
                .sslConfiguration(leaderConfig.sslConfiguration())
                .build();
        ServiceCreator creator = ServiceCreator.noPayloadLimiter(
                metricsManager, Refreshable.only(serverListConfig), userAgent, () -> RemotingClientConfigs.DEFAULT);
        LockService remoteLock =
                new RemoteLockServiceAdapter(creator.createService(NamespaceAgnosticLockRpcClient.class));
        TimestampService remoteTime = creator.createService(TimestampService.class);
        TimestampManagementService remoteManagement = creator.createService(TimestampManagementService.class);

        if (leaderConfig.leaders().size() == 1) {
            // Attempting to connect to ourself while processing a request can lead to deadlock if incoming request
            // volume is high, as all Jetty threads end up waiting for the timestamp server, and no threads remain to
            // actually handle the timestamp server requests. If we are the only single leader, we can avoid the
            // deadlock entirely; so use PingableLeader's getUUID() to detect this situation and eliminate the redundant
            // call.

            PingableLeader localPingableLeader = localPaxosServices.localPingableLeader();
            String localServerId = localPingableLeader.getUUID();
            PingableLeader remotePingableLeader = AtlasDbHttpClients.createProxy(
                    ServiceCreator.createTrustContext(leaderConfig.sslConfiguration()),
                    Iterables.getOnlyElement(leaderConfig.leaders()),
                    PingableLeader.class,
                    AuxiliaryRemotingParameters.builder()
                            .userAgent(userAgent)
                            .shouldRetry(true)
                            .shouldLimitPayload(true)
                            .shouldUseExtendedTimeout(false)
                            .build());

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
                    } catch (UnknownRemoteException e) {
                        // This is done to replicate previous behaviour, where 4xxs returned to a JaxRsClient would
                        // manifest as ClientErrorExceptions.
                        if (400 <= e.getStatus() && e.getStatus() <= 499) {
                            useLocalServicesFuture.complete(false);
                            return;
                        }
                        logAfter = logFailureToReadRemoteTimestampServerId(logAfter, e);
                    } catch (Throwable e) {
                        logAfter = logFailureToReadRemoteTimestampServerId(logAfter, e);
                    }
                    Uninterruptibles.sleepUninterruptibly(Duration.ofSeconds(1));
                }
            });

            // Create dynamic service proxies, that switch to talking directly to our local services if it turns out our
            // remote services are pointed at them anyway.
            LockService dynamicLockService = LocalOrRemoteProxy.newProxyInstance(
                    LockService.class, localLock, remoteLock, useLocalServicesFuture);

            // Use managedTimestampProxy here to avoid local calls going through indirection.
            TimestampService dynamicTimeService = LocalOrRemoteProxy.newProxyInstance(
                    TimestampService.class, managedTimestampProxy, remoteTime, useLocalServicesFuture);
            TimestampManagementService dynamicManagementService = LocalOrRemoteProxy.newProxyInstance(
                    TimestampManagementService.class, managedTimestampProxy, remoteManagement, useLocalServicesFuture);
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

    private static TimestampService getTimestampFacade(ManagedTimestampService managedTimestampService) {
        return new TimestampService() {
            @Override
            public long getFreshTimestamp() {
                return managedTimestampService.getFreshTimestamp();
            }

            @Override
            public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
                return managedTimestampService.getFreshTimestamps(numTimestampsRequested);
            }
        };
    }

    private static TimestampManagementService getTimestampManagementFacade(
            ManagedTimestampService managedTimestampService) {
        return new TimestampManagementService() {
            @Override
            public void fastForwardTimestamp(long currentTimestamp) {
                managedTimestampService.fastForwardTimestamp(currentTimestamp);
            }

            @Override
            public String ping() {
                return managedTimestampService.ping();
            }
        };
    }

    private static int logFailureToReadRemoteTimestampServerId(int logAfter, Throwable th) {
        if (logAfter == 0) {
            log.warn("Failed to read remote timestamp server ID", th);
            return LOGGING_INTERVAL;
        }
        return logAfter - 1;
    }

    private static LockAndTimestampServices createRawRemoteServices(
            MetricsManager metricsManager,
            AtlasDbConfig config,
            Supplier<AtlasDbRuntimeConfig> runtimeConfigSupplier,
            UserAgent userAgent) {
        ServiceCreator creator = ServiceCreator.noPayloadLimiter(
                metricsManager, () -> config.lock().get(), userAgent, () -> runtimeConfigSupplier
                        .get()
                        .remotingClient());
        LockService lockService =
                new RemoteLockServiceAdapter(creator.createService(NamespaceAgnosticLockRpcClient.class));
        TimestampService timeService = creator.createService(TimestampService.class);
        TimestampManagementService timestampManagementService = creator.createService(TimestampManagementService.class);

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
            Supplier<LockService> lock,
            Supplier<ManagedTimestampService> managedTimestampServiceSupplier) {
        LockService lockService =
                ServiceCreator.instrumentService(metricsManager.getRegistry(), lock.get(), LockService.class);

        ManagedTimestampService managedTimestampService = managedTimestampServiceSupplier.get();

        TimestampService timeService = ServiceCreator.instrumentService(
                metricsManager.getRegistry(), managedTimestampService, TimestampService.class);
        TimestampManagementService timestampManagementService = ServiceCreator.instrumentService(
                metricsManager.getRegistry(), managedTimestampService, TimestampManagementService.class);

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

    private static MultiTableSweepQueueWriter uninitializedTargetedSweeper(
            MetricsManager metricsManager,
            TargetedSweepInstallConfig install,
            Follower follower,
            Supplier<TargetedSweepRuntimeConfig> runtime) {
        if (!install.enableSweepQueueWrites()) {
            return MultiTableSweepQueueWriter.NO_OP;
        }
        return TargetedSweeper.createUninitialized(metricsManager, runtime, install, ImmutableList.of(follower));
    }

    @Value.Immutable
    @Value.Style(stagedBuilder = false)
    public interface LockAndTimestampServices {
        LockService lock();

        TimestampService timestamp();

        TimestampManagementService timestampManagement();

        TimelockService timelock();

        Optional<TimeLockMigrator> migrator();

        @Value.Default
        default LockWatchManager lockWatcher() {
            return NoOpLockWatchManager.create(eventCache());
        }

        @Value.Default
        default LockWatchEventCache eventCache() {
            return NoOpLockWatchEventCache.create();
        }

        @Value.Derived
        default ManagedTimestampService managedTimestampService() {
            return new DelegatingManagedTimestampService(timestamp(), timestampManagement());
        }

        @SuppressWarnings("checkstyle:WhitespaceAround")
        @Value.Default
        default List<Runnable> resources() {
            return Collections.emptyList();
        }
    }

    @Value.Immutable
    public interface TransactionComponents {
        TransactionService transactionService();

        Optional<TransactionSchemaInstaller> schemaInstaller();
    }
}
