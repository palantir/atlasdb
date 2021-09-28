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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbRuntimeConfig;
import com.palantir.atlasdb.config.AuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.ImmutableServerListConfig;
import com.palantir.atlasdb.config.ImmutableTimeLockClientConfig;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.config.RemotingClientConfigs;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.config.ServerListConfigs;
import com.palantir.atlasdb.config.TimeLockClientConfig;
import com.palantir.atlasdb.config.TimeLockRequestBatcherProviders;
import com.palantir.atlasdb.debug.LockDiagnosticComponents;
import com.palantir.atlasdb.debug.LockDiagnosticConjureTimelockService;
import com.palantir.atlasdb.factory.Leaders.LocalPaxosServices;
import com.palantir.atlasdb.factory.startup.TimeLockMigrator;
import com.palantir.atlasdb.factory.timelock.TimestampCorroboratingTimelockService;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.keyvalue.api.LockWatchCachingConfig;
import com.palantir.atlasdb.keyvalue.api.watch.LockWatchManagerImpl;
import com.palantir.atlasdb.keyvalue.api.watch.LockWatchManagerInternal;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.timelock.api.ConjureTimelockService;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.transaction.impl.InstrumentedTimelockService;
import com.palantir.atlasdb.transaction.impl.TimelockTimestampServiceAdapter;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.api.errors.UnknownRemoteException;
import com.palantir.dialogue.clients.DialogueClients.ReloadingFactory;
import com.palantir.leader.PingableLeader;
import com.palantir.leader.proxy.AwaitingLeadershipProxy;
import com.palantir.leader.proxy.LeadershipCoordinator;
import com.palantir.lock.LockRpcClient;
import com.palantir.lock.LockService;
import com.palantir.lock.NamespaceAgnosticLockRpcClient;
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
import com.palantir.lock.v2.NamespacedTimelockRpcClient;
import com.palantir.lock.v2.TimelockRpcClient;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.watch.LockWatchCache;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.refreshable.Refreshable;
import com.palantir.timestamp.ManagedTimestampService;
import com.palantir.timestamp.RemoteTimestampManagementAdapter;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampRange;
import com.palantir.timestamp.TimestampService;
import com.palantir.timestamp.TimestampStoreInvalidator;
import com.palantir.util.OptionalResolver;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.ws.rs.ClientErrorException;

public final class LockAndTimestampServiceFactory {
    private static final SafeLogger log = SafeLoggerFactory.get(LockAndTimestampServiceFactory.class);

    private static final int ATTEMPTS_BEFORE_LOGGING_FAILURE_TO_READ_REMOTE_TIMESTAMP_SERVER_ID = 60;

    private LockAndTimestampServiceFactory() {}

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
            Optional<TimeLockRequestBatcherProviders> timelockRequestBatcherProviders,
            Set<Schema> schemas) {
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
                timelockRequestBatcherProviders,
                schemas);
        return withMetrics(
                metricsManager,
                withCorroboratingTimestampService(
                        config.namespace(), metricsManager, withRefreshingLockService(lockAndTimestampServices)));
    }

    private static LockAndTimestampServices withCorroboratingTimestampService(
            Optional<String> userNamespace,
            MetricsManager metricsManager,
            LockAndTimestampServices lockAndTimestampServices) {
        TimelockService timelockService = TimestampCorroboratingTimelockService.create(
                userNamespace, metricsManager.getTaggedRegistry(), lockAndTimestampServices.timelock());
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

    static LockAndTimestampServices createRawInstrumentedServices(
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
            Optional<TimeLockRequestBatcherProviders> timelockRequestBatcherProviders,
            Set<Schema> knownSchemas) {
        AtlasDbRuntimeConfig initialRuntimeConfig = runtimeConfig.get();
        assertNoSpuriousTimeLockBlockInRuntimeConfig(config, initialRuntimeConfig);
        if (config.leader().isPresent()) {
            return createRawLeaderServices(metricsManager, config.leader().get(), env, lock, time, userAgent);
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
            Optional<TimeLockRequestBatcherProviders> timelockRequestBatcherProviders,
            Set<Schema> schemas,
            LockWatchCachingConfig cachingConfig) {
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

        NamespacedConjureLockWatchingService lockWatchingService = new NamespacedConjureLockWatchingService(
                serviceProvider.getConjureLockWatchingService(), timelockNamespace);

        LockWatchManagerInternal lockWatchManager =
                LockWatchManagerImpl.create(metricsManager, schemas, lockWatchingService, cachingConfig);

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
                        lockWatchManager.getCache(),
                        multiClientTimelockServiceSupplier));
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

    private static RequestBatchersFactory getRequestBatchersFactory(
            String namespace,
            Optional<TimeLockRequestBatcherProviders> timelockRequestBatcherProviders,
            LockWatchCache lockWatchCache,
            Supplier<InternalMultiClientConjureTimelockService> multiClientTimelockServiceSupplier) {
        return RequestBatchersFactory.create(
                lockWatchCache,
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
            TransactionManagers.runAsync.accept(() -> {
                int attemptsLeftBeforeLog = ATTEMPTS_BEFORE_LOGGING_FAILURE_TO_READ_REMOTE_TIMESTAMP_SERVER_ID;
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
                        attemptsLeftBeforeLog = logFailureToReadRemoteTimestampServerId(attemptsLeftBeforeLog, e);
                    } catch (Throwable e) {
                        attemptsLeftBeforeLog = logFailureToReadRemoteTimestampServerId(attemptsLeftBeforeLog, e);
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
                    .timelock(new LegacyTimelockService(
                            dynamicTimeService, dynamicLockService, TransactionManagers.LOCK_CLIENT))
                    .build();

        } else {
            return ImmutableLockAndTimestampServices.builder()
                    .lock(remoteLock)
                    .timestamp(remoteTime)
                    .timestampManagement(remoteManagement)
                    .timelock(new LegacyTimelockService(remoteTime, remoteLock, TransactionManagers.LOCK_CLIENT))
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

    private static int logFailureToReadRemoteTimestampServerId(int attemptsLeftBeforeLog, Throwable th) {
        if (attemptsLeftBeforeLog == 0) {
            log.warn("Failed to read remote timestamp server ID", th);
            return ATTEMPTS_BEFORE_LOGGING_FAILURE_TO_READ_REMOTE_TIMESTAMP_SERVER_ID;
        }
        return attemptsLeftBeforeLog - 1;
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
                .timelock(new LegacyTimelockService(timeService, lockService, TransactionManagers.LOCK_CLIENT))
                .build();
    }
}
