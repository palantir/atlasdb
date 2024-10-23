/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

import com.google.common.base.Suppliers;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbRuntimeConfig;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.config.ServerListConfigs;
import com.palantir.atlasdb.config.TimeLockClientConfig;
import com.palantir.atlasdb.config.TimeLockRequestBatcherProviders;
import com.palantir.atlasdb.debug.LockDiagnosticComponents;
import com.palantir.atlasdb.debug.LockDiagnosticConjureTimelockService;
import com.palantir.atlasdb.factory.startup.TimeLockMigrator;
import com.palantir.atlasdb.keyvalue.api.LockWatchCachingConfig;
import com.palantir.atlasdb.keyvalue.api.watch.LockWatchManagerInternal;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.timelock.api.ConjureTimelockService;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.transaction.impl.InstrumentedTimelockService;
import com.palantir.atlasdb.transaction.impl.TimelockTimestampServiceAdapter;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.dialogue.clients.DialogueClients.ReloadingFactory;
import com.palantir.lock.LockService;
import com.palantir.lock.NamespaceAgnosticLockRpcClient;
import com.palantir.lock.client.AuthenticatedInternalMultiClientConjureTimelockService;
import com.palantir.lock.client.ImmutableMultiClientRequestBatchers;
import com.palantir.lock.client.InternalMultiClientConjureTimelockService;
import com.palantir.lock.client.LeaderElectionReportingTimelockService;
import com.palantir.lock.client.LeaderTimeCoalescingBatcher;
import com.palantir.lock.client.LeaderTimeGetter;
import com.palantir.lock.client.LegacyLeaderTimeGetter;
import com.palantir.lock.client.LegacyLockTokenUnlocker;
import com.palantir.lock.client.LockLeaseService;
import com.palantir.lock.client.LockTokenUnlocker;
import com.palantir.lock.client.MultiClientTimeLockUnlocker;
import com.palantir.lock.client.NamespacedCoalescingLeaderTimeGetter;
import com.palantir.lock.client.NamespacedConjureLockWatchTimeLockDiagnosticsService;
import com.palantir.lock.client.NamespacedConjureLockWatchingService;
import com.palantir.lock.client.NamespacedConjureTimelockService;
import com.palantir.lock.client.NamespacedLockTokenUnlocker;
import com.palantir.lock.client.ProfilingTimelockService;
import com.palantir.lock.client.ReferenceTrackingWrapper;
import com.palantir.lock.client.RemoteLockServiceAdapter;
import com.palantir.lock.client.RemoteTimelockServiceAdapter;
import com.palantir.lock.client.RequestBatchersFactory;
import com.palantir.lock.client.TimeLockClient;
import com.palantir.lock.client.TimestampCorroboratingTimelockService;
import com.palantir.lock.client.metrics.TimeLockFeedbackBackgroundTask;
import com.palantir.lock.client.timestampleases.MinLeasedTimestampGetter;
import com.palantir.lock.client.timestampleases.MinLeasedTimestampGetterImpl;
import com.palantir.lock.client.timestampleases.NamespacedTimestampLeaseService;
import com.palantir.lock.client.timestampleases.NamespacedTimestampLeaseServiceImpl;
import com.palantir.lock.client.timestampleases.TimestampLeaseAcquirer;
import com.palantir.lock.client.timestampleases.TimestampLeaseAcquirerImpl;
import com.palantir.lock.impl.LegacyTimelockService;
import com.palantir.lock.v2.DefaultNamespacedTimelockRpcClient;
import com.palantir.lock.v2.NamespacedTimelockRpcClient;
import com.palantir.lock.v2.TimelockRpcClient;
import com.palantir.lock.v2.TimelockService;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.refreshable.Refreshable;
import com.palantir.timestamp.ManagedTimestampService;
import com.palantir.timestamp.RemoteTimestampManagementAdapter;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampService;
import com.palantir.timestamp.TimestampStoreInvalidator;
import com.palantir.util.OptionalResolver;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;

public final class DefaultLockAndTimestampServiceFactory implements LockAndTimestampServiceFactory {
    private final MetricsManager metricsManager;
    private final AtlasDbConfig config;
    private final Refreshable<AtlasDbRuntimeConfig> runtimeConfig;
    private final Supplier<LockService> lock;
    private final Supplier<ManagedTimestampService> time;
    private final TimestampStoreInvalidator invalidator;
    private final UserAgent userAgent;
    private final Optional<LockDiagnosticComponents> lockDiagnosticComponents;
    private final ReloadingFactory reloadingFactory;
    private final Optional<TimeLockFeedbackBackgroundTask> timeLockFeedbackBackgroundTask;
    private final Optional<TimeLockRequestBatcherProviders> timelockRequestBatcherProviders;
    private final Set<Schema> schemas;

    private final BiFunction<TimelockService, TimestampManagementService, TimeLockClient> timelockClientFactory;

    public DefaultLockAndTimestampServiceFactory(
            MetricsManager metricsManager,
            AtlasDbConfig config,
            Refreshable<AtlasDbRuntimeConfig> runtimeConfig,
            Supplier<LockService> lock,
            Supplier<ManagedTimestampService> time,
            TimestampStoreInvalidator invalidator,
            UserAgent userAgent,
            Optional<LockDiagnosticComponents> lockDiagnosticComponents,
            ReloadingFactory reloadingFactory,
            Optional<TimeLockFeedbackBackgroundTask> timeLockFeedbackBackgroundTask,
            Optional<TimeLockRequestBatcherProviders> timelockRequestBatcherProviders,
            Set<Schema> schemas) {
        this(
                metricsManager,
                config,
                runtimeConfig,
                lock,
                time,
                invalidator,
                userAgent,
                lockDiagnosticComponents,
                reloadingFactory,
                timeLockFeedbackBackgroundTask,
                timelockRequestBatcherProviders,
                schemas,
                (service, _management) -> TimeLockClient.createDefault(service));
    }

    public DefaultLockAndTimestampServiceFactory(
            MetricsManager metricsManager,
            AtlasDbConfig config,
            Refreshable<AtlasDbRuntimeConfig> runtimeConfig,
            Supplier<LockService> lock,
            Supplier<ManagedTimestampService> time,
            TimestampStoreInvalidator invalidator,
            UserAgent userAgent,
            Optional<LockDiagnosticComponents> lockDiagnosticComponents,
            ReloadingFactory reloadingFactory,
            Optional<TimeLockFeedbackBackgroundTask> timeLockFeedbackBackgroundTask,
            Optional<TimeLockRequestBatcherProviders> timelockRequestBatcherProviders,
            Set<Schema> schemas,
            BiFunction<TimelockService, TimestampManagementService, TimeLockClient> timelockClientFactory) {
        this.metricsManager = metricsManager;
        this.config = config;
        this.runtimeConfig = runtimeConfig;
        this.lock = lock;
        this.time = time;
        this.invalidator = invalidator;
        this.userAgent = userAgent;
        this.lockDiagnosticComponents = lockDiagnosticComponents;
        this.reloadingFactory = reloadingFactory;
        this.timeLockFeedbackBackgroundTask = timeLockFeedbackBackgroundTask;
        this.timelockRequestBatcherProviders = timelockRequestBatcherProviders;
        this.schemas = schemas;
        this.timelockClientFactory = timelockClientFactory;
    }

    @Override
    public LockAndTimestampServices createLockAndTimestampServices() {
        LockAndTimestampServices lockAndTimestampServices = createRawInstrumentedServices(
                metricsManager,
                config,
                runtimeConfig,
                lock,
                time,
                invalidator,
                userAgent,
                lockDiagnosticComponents,
                reloadingFactory,
                timeLockFeedbackBackgroundTask,
                timelockRequestBatcherProviders,
                schemas);
        return withMetrics(metricsManager, withDecoratedLockService(lockAndTimestampServices));
    }

    private LockAndTimestampServices withDecoratedLockService(LockAndTimestampServices lockAndTimestampServices) {
        TimeLockClient timeLockClient = timelockClientFactory.apply(
                lockAndTimestampServices.timelock(), lockAndTimestampServices.managedTimestampService());
        ProfilingTimelockService profilingService = ProfilingTimelockService.create(timeLockClient);
        return ImmutableLockAndTimestampServices.builder()
                .from(lockAndTimestampServices)
                .timestamp(new TimelockTimestampServiceAdapter(profilingService))
                .timelock(profilingService)
                .lock(LockServices.wrapWithDefaultDecorators(lockAndTimestampServices.lock()))
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

    @SuppressWarnings("TooManyArguments") // Legacy
    static LockAndTimestampServices createRawInstrumentedServices(
            MetricsManager metricsManager,
            AtlasDbConfig config,
            Refreshable<AtlasDbRuntimeConfig> runtimeConfig,
            Supplier<LockService> lock,
            Supplier<ManagedTimestampService> time,
            TimestampStoreInvalidator invalidator,
            UserAgent userAgent,
            Optional<LockDiagnosticComponents> lockDiagnosticComponents,
            ReloadingFactory reloadingFactory,
            Optional<TimeLockFeedbackBackgroundTask> timeLockFeedbackBackgroundTask,
            Optional<TimeLockRequestBatcherProviders> timelockRequestBatcherProviders,
            Set<Schema> knownSchemas) {
        AtlasDbRuntimeConfig initialRuntimeConfig = runtimeConfig.get();
        assertNoSpuriousTimeLockBlockInRuntimeConfig(config, initialRuntimeConfig);
        if (config.leader().isPresent()) { // 1-node leader config
            // equivalent to raw embedded; multi-node leader is not supported
            return createRawEmbeddedServices(metricsManager, lock, time);
        } else if (config.timestamp().isPresent() && config.lock().isPresent()) {
            return createRawRemoteServices(metricsManager, config, runtimeConfig, userAgent);
        } else if (TransactionManagers.isUsingTimeLock(config, initialRuntimeConfig)) {
            return createRawServicesFromTimeLock(
                    metricsManager,
                    config,
                    runtimeConfig,
                    invalidator,
                    userAgent,
                    lockDiagnosticComponents,
                    reloadingFactory,
                    timeLockFeedbackBackgroundTask,
                    timelockRequestBatcherProviders,
                    knownSchemas);
        } else { // raw embedded, i.e. embedded with no leader config
            return createRawEmbeddedServices(metricsManager, lock, time);
        }
    }

    private static void assertNoSpuriousTimeLockBlockInRuntimeConfig(
            AtlasDbConfig config, AtlasDbRuntimeConfig initialRuntimeConfig) {
        // Note: The other direction (timelock install config without a runtime block) should be maintained for
        // backwards compatibility.
        if (config.remoteTimestampAndLockOrLeaderBlocksPresent()
                && initialRuntimeConfig.timelockRuntime().isPresent()) {
            throw new SafeIllegalStateException("Found a service configured not to use timelock, with a timelock"
                    + " block in the runtime config! This is unexpected. If you wish to use non-timelock services,"
                    + " please remove the timelock block from the runtime config; if you wish to use timelock,"
                    + " please remove the leader, remote timestamp or remote lock configuration blocks.");
        }
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
            Optional<TimeLockRequestBatcherProviders> timelockRequestBatcherProviders,
            Set<Schema> schemas) {
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
                timelockRequestBatcherProviders,
                schemas,
                config.lockWatchCaching());

        TimeLockMigrator migrator = TimeLockMigrator.create(
                lockAndTimestampServices.managedTimestampService(), invalidator, config.initializeAsync());
        migrator.migrate(); // This can proceed async if config.initializeAsync() was set

        return ImmutableLockAndTimestampServices.copyOf(lockAndTimestampServices)
                .withMigrator(migrator);
    }

    static Refreshable<ServerListConfig> getServerListConfigSupplierForTimeLock(
            AtlasDbConfig config, Refreshable<AtlasDbRuntimeConfig> runtimeConfigSupplier) {
        return ServerListConfigs.getTimeLockServersFromAtlasDbConfig(config, runtimeConfigSupplier);
    }

    private static LockAndTimestampServices getLockAndTimestampServices(
            MetricsManager metricsManager,
            Refreshable<ServerListConfig> timelockServerListConfig,
            UserAgent userAgent,
            String timelockNamespace,
            Optional<LockDiagnosticComponents> lockDiagnosticComponents,
            ReloadingFactory reloadingFactory,
            Optional<TimeLockFeedbackBackgroundTask> timeLockFeedbackBackgroundTask,
            Optional<TimeLockRequestBatcherProviders> timelockRequestBatcherProviders,
            Set<Schema> schemas,
            LockWatchCachingConfig cachingConfig) {
        AtlasDbDialogueServiceProvider serviceProvider = AtlasDbDialogueServiceProvider.create(
                timelockServerListConfig, reloadingFactory, userAgent, metricsManager.getTaggedRegistry());

        LockService lockService = LockServices.createRawLockServiceClient(
                serviceProvider, metricsManager.getRegistry(), timelockNamespace);

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
                new DefaultNamespacedTimelockRpcClient(timelockClient, timelockNamespace);
        LeaderElectionReportingTimelockService leaderElectionReportingTimelockService =
                LeaderElectionReportingTimelockService.create(withDiagnosticsConjureTimelockService, timelockNamespace);

        timeLockFeedbackBackgroundTask.ifPresent(
                task -> task.registerLeaderElectionStatistics(leaderElectionReportingTimelockService));

        NamespacedConjureTimelockService namespacedConjureTimelockService =
                TimestampCorroboratingTimelockService.create(
                        timelockNamespace, metricsManager.getTaggedRegistry(), leaderElectionReportingTimelockService);

        NamespacedConjureLockWatchingService lockWatchingService = new NamespacedConjureLockWatchingService(
                serviceProvider.getConjureLockWatchingService(), timelockNamespace);

        NamespacedConjureLockWatchTimeLockDiagnosticsService lockWatchDiagnosticsService =
                new NamespacedConjureLockWatchTimeLockDiagnosticsService(
                        serviceProvider.getConjureLockWatchDiagnosticsService(), timelockNamespace);

        Supplier<InternalMultiClientConjureTimelockService> multiClientTimelockServiceSupplier =
                getMultiClientTimelockServiceSupplier(serviceProvider);

        Supplier<Optional<RequestBatchersFactory.MultiClientRequestBatchers>> requestBatcherProvider =
                () -> timelockRequestBatcherProviders.map(batcherProviders -> ImmutableMultiClientRequestBatchers.of(
                        batcherProviders.commitTimestamps().getBatcher(multiClientTimelockServiceSupplier),
                        batcherProviders.startTransactions().getBatcher(multiClientTimelockServiceSupplier)));

        TimeLockHelperServices timeLockHelperServices = TimeLockHelperServices.create(
                timelockNamespace,
                metricsManager,
                schemas,
                lockWatchingService,
                cachingConfig,
                requestBatcherProvider,
                lockWatchDiagnosticsService);
        LockWatchManagerInternal lockWatchManager = timeLockHelperServices.lockWatchManager();

        RemoteTimelockServiceAdapter remoteTimelockServiceAdapter = createRemoteTimelockServiceAdapter(
                timelockNamespace,
                timelockRequestBatcherProviders,
                namespacedConjureTimelockService,
                multiClientTimelockServiceSupplier,
                namespacedTimelockRpcClient,
                timeLockHelperServices.requestBatchersFactory());

        TimestampManagementService timestampManagementService = new RemoteTimestampManagementAdapter(
                serviceProvider.getTimestampManagementRpcClient(), timelockNamespace);

        return ImmutableLockAndTimestampServices.builder()
                .lock(lockService)
                .timestamp(new TimelockTimestampServiceAdapter(remoteTimelockServiceAdapter))
                .timestampManagement(timestampManagementService)
                .timelock(remoteTimelockServiceAdapter)
                .lockWatcher(lockWatchManager)
                .addResources(remoteTimelockServiceAdapter::close)
                .addResources(lockWatchManager::close)
                .build();
    }

    private static RemoteTimelockServiceAdapter createRemoteTimelockServiceAdapter(
            String timelockNamespace,
            Optional<TimeLockRequestBatcherProviders> timelockRequestBatcherProviders,
            NamespacedConjureTimelockService namespacedConjureTimelockService,
            Supplier<InternalMultiClientConjureTimelockService> multiClientTimelockServiceSupplier,
            NamespacedTimelockRpcClient namespacedTimelockRpcClient,
            RequestBatchersFactory batchersFactory) {
        LockTokenUnlocker unlocker = getTimeLockUnlocker(
                timelockNamespace,
                timelockRequestBatcherProviders,
                namespacedConjureTimelockService,
                multiClientTimelockServiceSupplier);

        NamespacedTimestampLeaseService timestampLeaseService = new NamespacedTimestampLeaseServiceImpl(
                Namespace.of(timelockNamespace), multiClientTimelockServiceSupplier.get());

        TimestampLeaseAcquirer timestampLeaseAcquirer =
                TimestampLeaseAcquirerImpl.create(timestampLeaseService, unlocker);

        MinLeasedTimestampGetter minLeasedTimestampGetter = new MinLeasedTimestampGetterImpl(timestampLeaseService);

        LockLeaseService lockLeaseService = LockLeaseService.create(
                namespacedConjureTimelockService,
                getLeaderTimeGetter(
                        timelockNamespace,
                        timelockRequestBatcherProviders,
                        namespacedConjureTimelockService,
                        multiClientTimelockServiceSupplier),
                unlocker);

        return RemoteTimelockServiceAdapter.create(
                namespacedTimelockRpcClient,
                namespacedConjureTimelockService,
                batchersFactory,
                lockLeaseService,
                timestampLeaseAcquirer,
                minLeasedTimestampGetter);
    }

    // Note: There is some duplication in the following two methods, but extracting a common method requires a fairly
    // large amount of nontrivial state. Consider extracting a common method if this needs to be implemented again.
    private static LockTokenUnlocker getTimeLockUnlocker(
            String timelockNamespace,
            Optional<TimeLockRequestBatcherProviders> timelockRequestBatcherProviders,
            NamespacedConjureTimelockService namespacedConjureTimelockService,
            Supplier<InternalMultiClientConjureTimelockService> multiClientTimelockServiceSupplier) {
        if (timelockRequestBatcherProviders.isEmpty()) {
            return new LegacyLockTokenUnlocker(namespacedConjureTimelockService);
        }
        ReferenceTrackingWrapper<MultiClientTimeLockUnlocker> batcher =
                timelockRequestBatcherProviders.get().unlock().getBatcher(multiClientTimelockServiceSupplier);
        batcher.recordReference();
        return new NamespacedLockTokenUnlocker(timelockNamespace, batcher);
    }

    private static LeaderTimeGetter getLeaderTimeGetter(
            String timelockNamespace,
            Optional<TimeLockRequestBatcherProviders> timelockRequestBatcherProviders,
            NamespacedConjureTimelockService namespacedConjureTimelockService,
            Supplier<InternalMultiClientConjureTimelockService> multiClientTimelockServiceSupplier) {

        if (!timelockRequestBatcherProviders.isPresent()) {
            return new LegacyLeaderTimeGetter(namespacedConjureTimelockService);
        }

        ReferenceTrackingWrapper<LeaderTimeCoalescingBatcher> referenceTrackingBatcher =
                timelockRequestBatcherProviders.get().leaderTime().getBatcher(multiClientTimelockServiceSupplier);
        referenceTrackingBatcher.recordReference();
        return new NamespacedCoalescingLeaderTimeGetter(timelockNamespace, referenceTrackingBatcher);
    }

    private static Supplier<InternalMultiClientConjureTimelockService> getMultiClientTimelockServiceSupplier(
            AtlasDbDialogueServiceProvider serviceProvider) {
        return Suppliers.memoize(() -> new AuthenticatedInternalMultiClientConjureTimelockService(
                serviceProvider.getMultiClientConjureTimelockService()));
    }

    private static LockAndTimestampServices createRawRemoteServices(
            MetricsManager metricsManager,
            AtlasDbConfig config,
            Supplier<AtlasDbRuntimeConfig> runtimeConfigSupplier,
            UserAgent userAgent) {
        ServiceCreator creator = ServiceCreator.noPayloadLimiter(
                metricsManager,
                Refreshable.only(config.lock()
                        .orElseThrow(() -> new SafeIllegalArgumentException("lock server list config absent"))),
                userAgent,
                () -> runtimeConfigSupplier.get().remotingClient());
        LockService lockService =
                new RemoteLockServiceAdapter(creator.createService(NamespaceAgnosticLockRpcClient.class));
        TimestampService timeService = creator.createService(TimestampService.class);
        TimestampManagementService timestampManagementService = creator.createService(TimestampManagementService.class);

        return ImmutableLockAndTimestampServices.builder()
                .lock(lockService)
                .timestamp(timeService)
                .timestampManagement(timestampManagementService)
                .timelock(new LegacyTimelockService(timeService, lockService, TransactionManagers.LOCK_CLIENT))
                .build();
    }

    private static LockAndTimestampServices createRawEmbeddedServices(
            MetricsManager metricsManager,
            Supplier<LockService> lock,
            Supplier<ManagedTimestampService> managedTimestampServiceSupplier) {
        LockService lockService =
                ServiceCreator.instrumentService(metricsManager.getRegistry(), lock.get(), LockService.class);

        ManagedTimestampService managedTimestampService = managedTimestampServiceSupplier.get();

        TimestampService timeService = ServiceCreator.instrumentService(
                metricsManager.getRegistry(), managedTimestampService, TimestampService.class);
        TimestampManagementService timestampManagementService = ServiceCreator.instrumentService(
                metricsManager.getRegistry(), managedTimestampService, TimestampManagementService.class);

        return ImmutableLockAndTimestampServices.builder()
                .lock(lockService)
                .timestamp(timeService)
                .timestampManagement(timestampManagementService)
                .timelock(new LegacyTimelockService(timeService, lockService, TransactionManagers.LOCK_CLIENT))
                .build();
    }
}
