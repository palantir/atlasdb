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
package com.palantir.timelock.paxos;

import java.net.URL;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.config.AuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.ImmutableLeaderConfig;
import com.palantir.atlasdb.config.ImmutableServerListConfig;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.config.RemotingClientConfigs;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.http.BlockingTimeoutExceptionMapper;
import com.palantir.atlasdb.http.NotCurrentLeaderExceptionMapper;
import com.palantir.atlasdb.http.RedirectRetryTargeter;
import com.palantir.atlasdb.timelock.AsyncTimelockService;
import com.palantir.atlasdb.timelock.ConjureLockWatchingResource;
import com.palantir.atlasdb.timelock.ConjureTimelockResource;
import com.palantir.atlasdb.timelock.TimeLockResource;
import com.palantir.atlasdb.timelock.TimeLockServices;
import com.palantir.atlasdb.timelock.TimelockNamespaces;
import com.palantir.atlasdb.timelock.TooManyRequestsExceptionMapper;
import com.palantir.atlasdb.timelock.adjudicate.FeedbackHandler;
import com.palantir.atlasdb.timelock.adjudicate.HealthStatusReport;
import com.palantir.atlasdb.timelock.adjudicate.TimeLockClientFeedbackResource;
import com.palantir.atlasdb.timelock.lock.LockLog;
import com.palantir.atlasdb.timelock.lock.v1.ConjureLockV1Resource;
import com.palantir.atlasdb.timelock.management.PersistentNamespaceContexts;
import com.palantir.atlasdb.timelock.management.TimeLockManagementResource;
import com.palantir.atlasdb.timelock.paxos.ImmutableTimelockPaxosInstallationContext;
import com.palantir.atlasdb.timelock.paxos.PaxosResources;
import com.palantir.atlasdb.timelock.paxos.PaxosResourcesFactory;
import com.palantir.atlasdb.timelock.paxos.TimeLockCorruptionComponents;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.conjure.java.api.config.service.ServicesConfigBlock;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.undertow.lib.UndertowService;
import com.palantir.dialogue.clients.DialogueClients;
import com.palantir.leader.health.LeaderElectionHealthReport;
import com.palantir.lock.LockService;
import com.palantir.paxos.Client;
import com.palantir.refreshable.Refreshable;
import com.palantir.sls.versions.OrderableSlsVersion;
import com.palantir.timelock.ServiceDiscoveringDatabaseTimeLockSupplier;
import com.palantir.timelock.config.DatabaseTsBoundPersisterConfiguration;
import com.palantir.timelock.config.PaxosTsBoundPersisterConfiguration;
import com.palantir.timelock.config.TimeLockInstallConfiguration;
import com.palantir.timelock.config.TimeLockRuntimeConfiguration;
import com.palantir.timelock.config.TsBoundPersisterConfiguration;
import com.palantir.timelock.corruption.detection.CorruptionNotifierResource;
import com.palantir.timelock.corruption.detection.JerseyCorruptionFilter;
import com.palantir.timelock.corruption.detection.UndertowCorruptionHandlerService;
import com.palantir.timelock.history.LocalHistoryLoader;
import com.palantir.timelock.history.remote.TimeLockPaxosHistoryProviderResource;
import com.palantir.timelock.history.sqlite.SqlitePaxosStateLogHistory;
import com.palantir.timelock.invariants.NoSimultaneousServiceCheck;
import com.palantir.timelock.invariants.TimeLockActivityCheckerFactory;
import com.palantir.timelock.management.ImmutableTimestampStorage;
import com.palantir.timelock.management.TimestampStorage;
import com.palantir.timestamp.ManagedTimestampService;
import com.zaxxer.hikari.HikariDataSource;

@SuppressWarnings("checkstyle:FinalClass") // This is mocked internally
public class TimeLockAgent {
    // Schema version from 2 onwards are on SQLite
    private static final Long SCHEMA_VERSION = 3L;

    private final MetricsManager metricsManager;
    private final TimeLockInstallConfiguration install;
    private final Supplier<TimeLockRuntimeConfiguration> runtime;
    private final Consumer<Object> registrar;
    private final Optional<Consumer<UndertowService>> undertowRegistrar;
    private final PaxosResources paxosResources;
    private final LockCreator lockCreator;
    private final TimestampStorage timestampStorage;
    private final TimeLockServicesCreator timelockCreator;
    private final NoSimultaneousServiceCheck noSimultaneousServiceCheck;
    private final HikariDataSource sqliteDataSource;
    private final FeedbackHandler feedbackHandler;
    private final TimeLockCorruptionComponents corruptionComponents;
    private LeaderPingHealthCheck healthCheck;
    private TimelockNamespaces namespaces;

    public static TimeLockAgent create(MetricsManager metricsManager,
            TimeLockInstallConfiguration install,
            Supplier<TimeLockRuntimeConfiguration> runtime,
            UserAgent userAgent,
            int threadPoolSize,
            long blockingTimeoutMs,
            Consumer<Object> registrar,
            Optional<Consumer<UndertowService>> undertowRegistrar,
            OrderableSlsVersion timeLockVersion) {
        TimeLockDialogueServiceProvider timeLockDialogueServiceProvider = createTimeLockDialogueServiceProvider(
                metricsManager, install, userAgent);
        PaxosResourcesFactory.TimelockPaxosInstallationContext installationContext =
                ImmutableTimelockPaxosInstallationContext
                        .of(install, userAgent, timeLockDialogueServiceProvider, timeLockVersion);

        PaxosResources paxosResources = PaxosResourcesFactory.create(
                installationContext,
                metricsManager,
                Suppliers.compose(TimeLockRuntimeConfiguration::paxos, runtime::get));

        TimeLockAgent agent = new TimeLockAgent(
                metricsManager,
                install,
                runtime,
                undertowRegistrar,
                threadPoolSize,
                blockingTimeoutMs,
                registrar,
                paxosResources,
                userAgent,
                installationContext.sqliteDataSource());
        agent.createAndRegisterResources();
        return agent;
    }

    private static TimeLockDialogueServiceProvider createTimeLockDialogueServiceProvider(
            MetricsManager metricsManager, TimeLockInstallConfiguration install, UserAgent userAgent) {
        DialogueClients.ReloadingFactory baseFactory = DialogueClients.create(
                Refreshable.only(ServicesConfigBlock.builder().build()))
                .withBlockingExecutor(PTExecutors.newCachedThreadPool("atlas-dialogue-blocking"));
        ServerListConfig timeLockServerListConfig = ImmutableServerListConfig.builder()
                .addAllServers(PaxosRemotingUtils.getRemoteServerPaths(install))
                .sslConfiguration(install.cluster().cluster().security())
                .proxyConfiguration(install.cluster().cluster().proxyConfiguration())
                .build();
        return TimeLockDialogueServiceProvider.create(
                metricsManager.getTaggedRegistry(),
                baseFactory,
                timeLockServerListConfig,
                AuxiliaryRemotingParameters.builder()
                        .userAgent(userAgent)
                        .shouldLimitPayload(false)
                        .remotingClientConfig(() -> RemotingClientConfigs.DEFAULT)
                        .shouldUseExtendedTimeout(false)
                        .build());
    }

    private TimeLockAgent(MetricsManager metricsManager,
            TimeLockInstallConfiguration install,
            Supplier<TimeLockRuntimeConfiguration> runtime,
            Optional<Consumer<UndertowService>> undertowRegistrar,
            int threadPoolSize,
            long blockingTimeoutMs,
            Consumer<Object> registrar,
            PaxosResources paxosResources,
            UserAgent userAgent, HikariDataSource sqliteDataSource) {
        this.metricsManager = metricsManager;
        this.install = install;
        this.runtime = runtime;
        this.undertowRegistrar = undertowRegistrar;
        this.registrar = registrar;
        this.paxosResources = paxosResources;
        this.sqliteDataSource = sqliteDataSource;
        this.lockCreator = new LockCreator(runtime, threadPoolSize, blockingTimeoutMs);
        this.timestampStorage = getTimestampStorage();
        LockLog lockLog = new LockLog(metricsManager.getRegistry(),
                Suppliers.compose(TimeLockRuntimeConfiguration::slowLockLogTriggerMillis, runtime::get));

        this.timelockCreator = new AsyncTimeLockServicesCreator(
                metricsManager,
                lockLog,
                paxosResources.leadershipComponents(),
                install.lockDiagnosticConfig()
        );

        this.noSimultaneousServiceCheck = NoSimultaneousServiceCheck.create(
                new TimeLockActivityCheckerFactory(install, metricsManager, userAgent).getTimeLockActivityCheckers());

        this.feedbackHandler = new FeedbackHandler(metricsManager, () -> runtime.get().adjudication().enabled());
        this.corruptionComponents = paxosResources.timeLockCorruptionComponents();
    }

    private TimestampStorage getTimestampStorage() {
        TsBoundPersisterConfiguration timestampBoundPersistence = install.timestampBoundPersistence();
        if (timestampBoundPersistence instanceof PaxosTsBoundPersisterConfiguration) {
            return createPaxosBasedTimestampStorage();
        } else if (timestampBoundPersistence instanceof DatabaseTsBoundPersisterConfiguration) {
            return createDatabaseTimestampStorage(
                    (DatabaseTsBoundPersisterConfiguration) timestampBoundPersistence);
        }
        throw new RuntimeException(String.format("Unknown TsBoundPersisterConfiguration found %s",
                timestampBoundPersistence.getClass()));
    }

    private TimestampStorage createPaxosBasedTimestampStorage() {
        return ImmutableTimestampStorage.builder()
                .timestampCreator(new PaxosTimestampCreator(paxosResources.timestampServiceFactory()))
                .persistentNamespaceContext(
                        PersistentNamespaceContexts.timestampBoundPaxos(
                                install.paxos().dataDirectory().toPath(),
                                sqliteDataSource))
                .build();
    }

    private TimestampStorage createDatabaseTimestampStorage(
            DatabaseTsBoundPersisterConfiguration timestampBoundPersistence) {
        ServiceDiscoveringDatabaseTimeLockSupplier dbTimeLockSupplier =
                new ServiceDiscoveringDatabaseTimeLockSupplier(
                        metricsManager, timestampBoundPersistence.keyValueServiceConfig(), createLeaderConfig());
        return ImmutableTimestampStorage.builder()
                .timestampCreator(new DbBoundTimestampCreator(dbTimeLockSupplier))
                .persistentNamespaceContext(PersistentNamespaceContexts.dbBound(
                        dbTimeLockSupplier.getTimestampSeriesProvider(
                                AtlasDbConstants.DB_TIMELOCK_TIMESTAMP_TABLE)))
                .build();
    }

    private void createAndRegisterResources() {
        registerTimeLockCorruptionJerseyFilter();
        registerTimeLockCorruptionNotifiers();
        registerPaxosResource();
        registerExceptionMappers();
        registerClientFeedbackService();

        namespaces = new TimelockNamespaces(
                metricsManager,
                this::createInvalidatingTimeLockServices,
                Suppliers.compose(TimeLockRuntimeConfiguration::maxNumberOfClients, runtime::get));
        registerManagementResource();
        // Finally, register the health check, and endpoints associated with the clients.
        TimeLockResource resource = TimeLockResource.create(namespaces);
        healthCheck = paxosResources.leadershipComponents().healthCheck(namespaces::getActiveClients);

        registrar.accept(resource);

        Function<String, AsyncTimelockService> asyncTimelockServiceGetter
                = namespace -> namespaces.get(namespace).getTimelockService();
        Function<String, LockService> lockServiceGetter
                = namespace -> namespaces.get(namespace).getLockService();
        LocalHistoryLoader localHistoryLoader = LocalHistoryLoader.create(
                SqlitePaxosStateLogHistory.create(sqliteDataSource));

        if (undertowRegistrar.isPresent()) {
            Consumer<UndertowService> presentUndertowRegistrar = undertowRegistrar.get();
            registerCorruptionHandlerWrappedService(presentUndertowRegistrar,
                    ConjureTimelockResource.undertow(redirectRetryTargeter(), asyncTimelockServiceGetter));
            registerCorruptionHandlerWrappedService(presentUndertowRegistrar,
                    ConjureLockWatchingResource.undertow(redirectRetryTargeter(), asyncTimelockServiceGetter));
            registerCorruptionHandlerWrappedService(presentUndertowRegistrar,
                    ConjureLockV1Resource.undertow(redirectRetryTargeter(), lockServiceGetter));
            registerCorruptionHandlerWrappedService(presentUndertowRegistrar,
                    TimeLockPaxosHistoryProviderResource.undertow(localHistoryLoader));
        } else {
            registrar.accept(ConjureTimelockResource.jersey(redirectRetryTargeter(), asyncTimelockServiceGetter));
            registrar.accept(ConjureLockWatchingResource.jersey(redirectRetryTargeter(), asyncTimelockServiceGetter));
            registrar.accept(ConjureLockV1Resource.jersey(redirectRetryTargeter(), lockServiceGetter));
            registrar.accept(TimeLockPaxosHistoryProviderResource.jersey(localHistoryLoader));
        }
    }

    private void registerClientFeedbackService() {
        if (undertowRegistrar.isPresent()) {
            registerCorruptionHandlerWrappedService(undertowRegistrar.get(),
                    TimeLockClientFeedbackResource.undertow(feedbackHandler, this::isLeaderForClient));
        } else {
            registrar.accept(TimeLockClientFeedbackResource.jersey(feedbackHandler,
                    this::isLeaderForClient));
        }
    }

    private void registerTimeLockCorruptionNotifiers() {
        if (undertowRegistrar.isPresent()) {
            registerCorruptionHandlerWrappedService(undertowRegistrar.get(),
                    CorruptionNotifierResource.undertow(corruptionComponents.remoteCorruptionDetector()));
        } else {
            registrar.accept(CorruptionNotifierResource.jersey(corruptionComponents.remoteCorruptionDetector()));
        }
    }

    private boolean isLeaderForClient(Client client) {
        Map<Client, HealthCheckResponse> healthCheckResponseMap = paxosResources
                .leadershipComponents()
                .getLocalHealthCheckPinger()
                .apply(ImmutableSet.of(client));
        return healthCheckResponseMap.get(client).isLeader();
    }

    private void registerManagementResource() {
        Path rootDataDirectory = install.paxos().dataDirectory().toPath();
        if (undertowRegistrar.isPresent()) {
            registerCorruptionHandlerWrappedService(undertowRegistrar.get(), TimeLockManagementResource.undertow(
                    timestampStorage.persistentNamespaceContext(),
                    namespaces,
                    redirectRetryTargeter()));
        } else {
            registrar.accept(TimeLockManagementResource.jersey(
                    timestampStorage.persistentNamespaceContext(),
                    namespaces,
                    redirectRetryTargeter()));
        }
    }

    private void registerTimeLockCorruptionJerseyFilter() {
        if (!undertowRegistrar.isPresent()) {
            registrar.accept(new JerseyCorruptionFilter(corruptionComponents.timeLockCorruptionHealthCheck()));
        }
    }

    private void registerCorruptionHandlerWrappedService(Consumer<UndertowService> presentUndertowRegistrar,
            UndertowService service) {
        presentUndertowRegistrar.accept(
                UndertowCorruptionHandlerService.of(service, corruptionComponents.timeLockCorruptionHealthCheck()));
    }

    @SuppressWarnings("unused") // used by external health checks
    public Optional<HealthCheckDigest> getStatus() {
        if (getNumberOfActiveClients() == 0) {
            return Optional.empty();
        }

        HealthCheckDigest status = healthCheck.getStatus();
        noSimultaneousServiceCheck.processHealthCheckDigest(status);

        return Optional.of(status);
    }

    @SuppressWarnings({"unused", "WeakerAccess"}) // used by external health checks
    public int getNumberOfActiveClients() {
        return namespaces.getNumberOfActiveClients();
    }

    @SuppressWarnings("unused") // used by external health checks
    public int getMaxNumberOfClients() {
        return namespaces.getMaxNumberOfClients();
    }

    @SuppressWarnings("unused")
    public long getSchemaVersion() {
        // So far there's only been one schema version. For future schema versions, we will have to persist the version
        // to disk somehow, so that we can check if var/data/paxos will have data in the expected format.
        return SCHEMA_VERSION;
    }

    @SuppressWarnings("unused")
    public long getLatestSchemaVersion() {
        return SCHEMA_VERSION;
    }

    // No runtime configuration at the moment.
    private void registerPaxosResource() {
        paxosResources.resourcesForRegistration().forEach(registrar::accept);
    }

    private void registerExceptionMappers() {
        registrar.accept(new BlockingTimeoutExceptionMapper());

        registrar.accept(new NotCurrentLeaderExceptionMapper(redirectRetryTargeter()));

        registrar.accept(new TooManyRequestsExceptionMapper());
    }

    private RedirectRetryTargeter redirectRetryTargeter() {
        URL localServer = PaxosRemotingUtils.convertAddressToUrl(install, install.cluster().localServer());
        List<URL> clusterUrls = PaxosRemotingUtils.convertAddressesToUrls(install, install.cluster().clusterMembers());
        return RedirectRetryTargeter.create(localServer, clusterUrls);
    }

    /**
     * Creates timestamp and lock services for the given client. It is expected that for each client there should
     * only be (up to) one active timestamp service, and one active lock service at any time.
     * @param client Client namespace to create the services for
     * @return Invalidating timestamp and lock services
     */
    private TimeLockServices createInvalidatingTimeLockServices(String client) {
        LeaderConfig leaderConfig = createLeaderConfig();

        Client typedClient = Client.of(client);

        Supplier<ManagedTimestampService> rawTimestampServiceSupplier = timestampStorage.timestampCreator()
                .createTimestampService(typedClient, leaderConfig);
        Supplier<LockService> rawLockServiceSupplier = lockCreator::createThreadPoolingLockService;
        return timelockCreator.createTimeLockServices(typedClient, rawTimestampServiceSupplier, rawLockServiceSupplier);
    }

    private LeaderConfig createLeaderConfig() {
        List<String> uris = install.cluster().clusterMembers();
        return ImmutableLeaderConfig.builder()
                .addLeaders(uris.toArray(new String[0]))
                .localServer(install.cluster().localServer())
                .sslConfiguration(PaxosRemotingUtils.getSslConfigurationOptional(install))
                .quorumSize(PaxosRemotingUtils.getQuorumSize(uris))
                .build();
    }

    public HealthStatusReport timeLockAdjudicationFeedback() {
        return feedbackHandler.getTimeLockHealthStatus();
    }

    public LeaderElectionHealthReport timeLockLeadershipHealthCheck() {
        return paxosResources.leadershipContextFactory().leaderElectionHealthCheck().leaderElectionRateHealthReport();
    }

    public void shutdown() {
        paxosResources.leadershipComponents().shutdown();
        sqliteDataSource.close();
        timestampStorage.close();
    }
}
