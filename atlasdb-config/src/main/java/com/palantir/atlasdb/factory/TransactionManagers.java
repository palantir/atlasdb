/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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
import com.google.common.collect.Lists;
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
import com.palantir.atlasdb.config.ImmutableTimeLockClientConfig;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.config.ServerListConfigs;
import com.palantir.atlasdb.config.SweepConfig;
import com.palantir.atlasdb.config.TargetedSweepInstallConfig;
import com.palantir.atlasdb.config.TargetedSweepRuntimeConfig;
import com.palantir.atlasdb.config.TimeLockClientConfig;
import com.palantir.atlasdb.factory.startup.ConsistencyCheckRunner;
import com.palantir.atlasdb.factory.timestamp.FreshTimestampSupplierAdapter;
import com.palantir.atlasdb.http.UserAgents;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.ProfilingKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.TracingKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.ValidatingQueryRewritingKeyValueService;
import com.palantir.atlasdb.logging.KvsProfilingLogger;
import com.palantir.atlasdb.memory.InMemoryAtlasDbConfig;
import com.palantir.atlasdb.persistentlock.CheckAndSetExceptionMapper;
import com.palantir.atlasdb.persistentlock.KvsBackedPersistentLockService;
import com.palantir.atlasdb.persistentlock.NoOpPersistentLockService;
import com.palantir.atlasdb.persistentlock.PersistentLockService;
import com.palantir.atlasdb.qos.QosClient;
import com.palantir.atlasdb.qos.QosService;
import com.palantir.atlasdb.qos.client.AtlasDbQosClient;
import com.palantir.atlasdb.qos.config.QosClientConfig;
import com.palantir.atlasdb.qos.ratelimit.QosRateLimiters;
import com.palantir.atlasdb.schema.generated.SweepTableFactory;
import com.palantir.atlasdb.schema.metadata.SchemaMetadataService;
import com.palantir.atlasdb.schema.metadata.SchemaMetadataServiceImpl;
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
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.timelock.hackweek.JamesTransactionService;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManager;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManagers;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManagers;
import com.palantir.atlasdb.transaction.impl.consistency.ImmutableTimestampCorroborationConsistencyCheck;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.common.annotation.Output;
import com.palantir.logsafe.SafeArg;
import com.palantir.remoting.api.config.service.ServiceConfiguration;
import com.palantir.remoting3.clients.ClientConfigurations;
import com.palantir.remoting3.jaxrs.JaxRsClient;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import com.palantir.util.JavaSuppliers;
import com.palantir.util.OptionalResolver;

@Value.Immutable
@Value.Style(stagedBuilder = true)
public abstract class TransactionManagers {
    private static final int LOGGING_INTERVAL = 60;
    private static final Logger log = LoggerFactory.getLogger(TransactionManagers.class);

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
    boolean allowHiddenTableAccess() {
        return false;
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

    private TransactionManager serializableInternal(@Output List<AutoCloseable> closeables) {
        MetricsManager metricsManager = MetricsManagers.of(globalMetricsRegistry(), globalTaggedMetricRegistry());
        final AtlasDbConfig config = config();
        checkInstallConfig(config);

        AtlasDbRuntimeConfig defaultRuntime = AtlasDbRuntimeConfig.defaultRuntimeConfig();
        Supplier<AtlasDbRuntimeConfig> runtimeConfigSupplier =
                () -> runtimeConfigSupplier().get().orElse(defaultRuntime);

        QosClient qosClient = initializeCloseable(() -> getQosClient(metricsManager,
                        JavaSuppliers.compose(AtlasDbRuntimeConfig::qos, runtimeConfigSupplier)), closeables);

        FreshTimestampSupplierAdapter adapter = new FreshTimestampSupplierAdapter();
        ServiceDiscoveringAtlasSupplier atlasFactory = new ServiceDiscoveringAtlasSupplier(metricsManager,
                        config.keyValueService(),
                        JavaSuppliers.compose(AtlasDbRuntimeConfig::keyValueService, runtimeConfigSupplier),
                        config.leader(), config.namespace(), Optional.empty(), config.initializeAsync(),
                        qosClient, adapter);

        JamesTransactionService james = null;

        adapter.setTimestampService(james);

        KvsProfilingLogger.setSlowLogThresholdMillis(config.getKvsSlowLogThresholdMillis());

        KeyValueService keyValueService = initializeCloseable(() -> {
            KeyValueService kvs = atlasFactory.getKeyValueService();
            kvs = ProfilingKeyValueService.create(kvs);

            kvs = TracingKeyValueService.create(kvs);
            kvs = AtlasDbMetrics.instrument(metricsManager.getRegistry(), KeyValueService.class,
                    kvs, MetricRegistry.name(KeyValueService.class));
            return ValidatingQueryRewritingKeyValueService.create(kvs);
        }, closeables);

        SchemaMetadataService schemaMetadataService = SchemaMetadataServiceImpl.create(keyValueService,
                config.initializeAsync());
        TransactionManagersInitializer initializer = TransactionManagersInitializer.createInitialTables(
                keyValueService, schemas(), schemaMetadataService, config.initializeAsync());
        PersistentLockService persistentLockService = createAndRegisterPersistentLockService(
                keyValueService, registrar(), config.initializeAsync());

        TransactionService transactionService = AtlasDbMetrics.instrument(metricsManager.getRegistry(),
                TransactionService.class, TransactionServices.createTransactionService(keyValueService));
        ConflictDetectionManager conflictManager = ConflictDetectionManagers.create(keyValueService);
        SweepStrategyManager sweepStrategyManager = SweepStrategyManagers.createDefault(keyValueService);

        CleanupFollower follower = CleanupFollower.create(schemas());

        Cleaner cleaner = initializeCloseable(() -> new DefaultCleanerBuilder(
                        keyValueService,
                        james,
                        ImmutableList.of(follower),
                        transactionService)
                        .setBackgroundScrubAggressively(config.backgroundScrubAggressively())
                        .setBackgroundScrubBatchSize(config.getBackgroundScrubBatchSize())
                        .setBackgroundScrubFrequencyMillis(config.getBackgroundScrubFrequencyMillis())
                        .setBackgroundScrubThreads(config.getBackgroundScrubThreads())
                        .setPunchIntervalMillis(config.getPunchIntervalMillis())
                        .setTransactionReadTimeout(config.getTransactionReadTimeoutMillis())
                        .setInitializeAsync(config.initializeAsync())
                        .buildCleaner(),
                closeables);

        MultiTableSweepQueueWriter targetedSweep = initializeCloseable(
                () -> uninitializedTargetedSweeper(metricsManager, config.targetedSweep(), follower,
                        JavaSuppliers.compose(AtlasDbRuntimeConfig::targetedSweep, runtimeConfigSupplier)),
                closeables);

        Callback<TransactionManager> callbacks = new Callback.CallChain<>(ImmutableList.of(
                timelockConsistencyCheckCallback(config, runtimeConfigSupplier.get(), james),
                targetedSweep.singleAttemptCallback(),
                asyncInitializationCallback()));

        TransactionManager transactionManager = initializeCloseable(
                () -> SerializableTransactionManager.create(
                        metricsManager,
                        keyValueService,
                        james,
                        transactionService,
                        Suppliers.ofInstance(AtlasDbConstraintCheckingMode.FULL_CONSTRAINT_CHECKING_THROWS_EXCEPTIONS),
                        conflictManager,
                        sweepStrategyManager,
                        cleaner,
                        () -> areTransactionManagerInitializationPrerequisitesSatisfied(initializer),
                        allowHiddenTableAccess(),
                        () -> runtimeConfigSupplier.get().transaction().getLockAcquireTimeoutMillis(),
                        config.keyValueService().concurrentGetRangesThreadPoolSize(),
                        config.keyValueService().defaultGetRangesConcurrency(),
                        config.initializeAsync(),
                        new TimestampCache(metricsManager.getRegistry(),
                                () -> runtimeConfigSupplier.get().getTimestampCacheSize()),
                        targetedSweep,
                        callbacks),
                closeables);
        TransactionManager instrumentedTransactionManager =
                AtlasDbMetrics.instrument(metricsManager.getRegistry(), TransactionManager.class, transactionManager);

        instrumentedTransactionManager.registerClosingCallback(qosClient::close);

        PersistentLockManager persistentLockManager = initializeCloseable(
                () -> new PersistentLockManager(
                        metricsManager, persistentLockService, config.getSweepPersistentLockWaitMillis()),
                closeables);
        initializeCloseable(
                () -> initializeSweepEndpointAndBackgroundProcess(
                        metricsManager,
                        config,
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
                        keyValueService,
                        instrumentedTransactionManager,
                        JavaSuppliers.compose(AtlasDbRuntimeConfig::compact, runtimeConfigSupplier)),
                closeables);

        return instrumentedTransactionManager;
    }

    private Optional<BackgroundCompactor> initializeCompactBackgroundProcess(
            MetricsManager metricsManager,
            KeyValueService keyValueService,
            TransactionManager transactionManager,
            Supplier<CompactorConfig> compactorConfigSupplier) {
        Optional<BackgroundCompactor> backgroundCompactorOptional = BackgroundCompactor.createAndRun(
                metricsManager,
                transactionManager,
                keyValueService,
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

    private QosClient getQosClient(MetricsManager metricsManager, Supplier<QosClientConfig> config) {
        Optional<ServiceConfiguration> qosServiceConfig = config.get().qosService();
        QosRateLimiters rateLimiters;
        if (qosServiceConfig.isPresent()) {
            QosService qosService = JaxRsClient.create(QosService.class, userAgent(),
                    ClientConfigurations.of(qosServiceConfig.get()));
            rateLimiters = QosRateLimiters.create(
                    JavaSuppliers.compose(conf -> conf.maxBackoffSleepTime().toMilliseconds(), config),
                    () -> qosService.readLimit(config().getNamespaceString()),
                    () -> qosService.writeLimit(config().getNamespaceString()));
        } else {
            rateLimiters = QosRateLimiters.create(
                    JavaSuppliers.compose(conf -> conf.maxBackoffSleepTime().toMilliseconds(), config),
                    JavaSuppliers.compose(conf -> conf.limits().readBytesPerSecond(), config),
                    JavaSuppliers.compose(conf -> conf.limits().writeBytesPerSecond(), config));
        }
        return AtlasDbQosClient.create(metricsManager, rateLimiters);
    }

    private static boolean areTransactionManagerInitializationPrerequisitesSatisfied(
            AsyncInitializer initializer) {
        return initializer.isInitialized();
    }

    private static void checkInstallConfig(AtlasDbConfig config) {
        if (config.getSweepBatchSize() != null
                || config.getSweepCellBatchSize() != null
                || config.getSweepReadLimit() != null
                || config.getSweepCandidateBatchHint() != null
                || config.getSweepDeleteBatchHint() != null) {
            log.warn("Your configuration specifies sweep parameters on the install config. They will be ignored."
                    + " Please use the runtime config to specify them.");
        }
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
                () -> getSweepBatchConfig(runtimeConfigSupplier.get().sweep(), config.keyValueService()));

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

    private static SweepBatchConfig getSweepBatchConfig(SweepConfig sweepConfig, KeyValueServiceConfig kvsConfig) {
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
            JamesTransactionService james) {
        if (isUsingTimeLock(atlasDbConfig, initialRuntimeConfig)) {
            // Only do the consistency check if we're using TimeLock.
            // This avoids a bootstrapping problem with leader-block services without async initialisation,
            // where you need a working timestamp service to check consistency, you need to check consistency
            // before you can return a TM, you need to return a TM to listen on ports, and you need to listen on
            // ports in order to get a working timestamp service.
            return ConsistencyCheckRunner.create(
                    ImmutableTimestampCorroborationConsistencyCheck.builder()
                            .conservativeBound(TransactionManager::getUnreadableTimestamp)
                            .freshTimestampSource(unused -> james.getFreshTimestamp().getTimestamp())
                            .build());
        }
        return Callback.noOp();
    }

    private static boolean isUsingTimeLock(AtlasDbConfig atlasDbConfig, AtlasDbRuntimeConfig runtimeConfig) {
        return atlasDbConfig.timelock().isPresent() || runtimeConfig.timelockRuntime().isPresent();
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

    private MultiTableSweepQueueWriter uninitializedTargetedSweeper(
                MetricsManager metricsManager,
            TargetedSweepInstallConfig config,
            Follower follower, Supplier<TargetedSweepRuntimeConfig> runtime) {
        if (config.enableSweepQueueWrites()) {
            return TargetedSweeper.createUninitialized(
                    metricsManager,
                    JavaSuppliers.compose(TargetedSweepRuntimeConfig::enabled, runtime),
                    JavaSuppliers.compose(TargetedSweepRuntimeConfig::shards, runtime),
                    config.conservativeThreads(),
                    config.thoroughThreads(),
                    ImmutableList.of(follower));
        }
        return MultiTableSweepQueueWriter.NO_OP;
    }
}
