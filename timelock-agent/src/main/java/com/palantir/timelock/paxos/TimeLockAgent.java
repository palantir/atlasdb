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

import static com.palantir.atlasdb.timelock.paxos.PaxosTimeLockConstants.ACCEPTOR_SUBDIRECTORY_PATH;
import static com.palantir.atlasdb.timelock.paxos.PaxosTimeLockConstants.LEADER_PAXOS_NAMESPACE;
import static com.palantir.atlasdb.timelock.paxos.PaxosTimeLockConstants.LEARNER_SUBDIRECTORY_PATH;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
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
import com.palantir.atlasdb.spi.KeyValueServiceRuntimeConfig;
import com.palantir.atlasdb.timelock.AsyncTimelockService;
import com.palantir.atlasdb.timelock.ConjureLockWatchingResource;
import com.palantir.atlasdb.timelock.ConjureTimelockResource;
import com.palantir.atlasdb.timelock.TimeLockResource;
import com.palantir.atlasdb.timelock.TimeLockServices;
import com.palantir.atlasdb.timelock.TimelockNamespaces;
import com.palantir.atlasdb.timelock.TooManyRequestsExceptionMapper;
import com.palantir.atlasdb.timelock.adjudicate.FeedbackHandler;
import com.palantir.atlasdb.timelock.adjudicate.HealthStatusReport;
import com.palantir.atlasdb.timelock.adjudicate.LeaderElectionMetricAggregator;
import com.palantir.atlasdb.timelock.adjudicate.TimeLockClientFeedbackResource;
import com.palantir.atlasdb.timelock.batch.MultiClientConjureTimelockResource;
import com.palantir.atlasdb.timelock.lock.LockLog;
import com.palantir.atlasdb.timelock.lock.v1.ConjureLockV1Resource;
import com.palantir.atlasdb.timelock.management.PersistentNamespaceContexts;
import com.palantir.atlasdb.timelock.management.ServiceLifecycleController;
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
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.paxos.Client;
import com.palantir.refreshable.Refreshable;
import com.palantir.sls.versions.OrderableSlsVersion;
import com.palantir.timelock.ServiceDiscoveringDatabaseTimeLockSupplier;
import com.palantir.timelock.config.ClusterConfiguration;
import com.palantir.timelock.config.DatabaseTsBoundPersisterConfiguration;
import com.palantir.timelock.config.DatabaseTsBoundPersisterRuntimeConfiguration;
import com.palantir.timelock.config.PaxosTsBoundPersisterConfiguration;
import com.palantir.timelock.config.RestrictedTimeLockRuntimeConfiguration;
import com.palantir.timelock.config.TimeLockInstallConfiguration;
import com.palantir.timelock.config.TimeLockPersistenceInvariants;
import com.palantir.timelock.config.TimeLockRuntimeConfiguration;
import com.palantir.timelock.config.TsBoundPersisterConfiguration;
import com.palantir.timelock.corruption.detection.CorruptionHealthReport;
import com.palantir.timelock.corruption.handle.CorruptionNotifierResource;
import com.palantir.timelock.corruption.handle.JerseyCorruptionFilter;
import com.palantir.timelock.corruption.handle.UndertowCorruptionHandlerService;
import com.palantir.timelock.history.remote.TimeLockPaxosHistoryProviderResource;
import com.palantir.timelock.invariants.NoSimultaneousServiceCheck;
import com.palantir.timelock.invariants.TimeLockActivityCheckerFactory;
import com.palantir.timelock.management.ImmutableTimestampStorage;
import com.palantir.timelock.management.TimestampStorage;
import com.palantir.timelock.store.PersistenceConfigStore;
import com.palantir.timelock.store.SqliteBlobStore;
import com.palantir.timestamp.ManagedTimestampService;
import com.zaxxer.hikari.HikariDataSource;
import java.net.URL;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@SuppressWarnings("checkstyle:FinalClass") // This is mocked internally
public class TimeLockAgent {
    private static final SafeLogger log = SafeLoggerFactory.get(TimeLockAgent.class);
    // Schema version from 2 onwards are on SQLite
    static final Long SCHEMA_VERSION = 3L;

    private final MetricsManager metricsManager;
    private final TimeLockInstallConfiguration install;
    private final Refreshable<TimeLockRuntimeConfiguration> runtime;
    private final ClusterConfiguration cluster;
    private final Consumer<Object> registrar;
    private final Optional<Consumer<UndertowService>> undertowRegistrar;
    private final PaxosResources paxosResources;
    private final LockCreator lockCreator;
    private final TimestampStorage timestampStorage;
    private final TimeLockServicesCreator timelockCreator;
    private final NoSimultaneousServiceCheck noSimultaneousServiceCheck;
    private final PersistedSchemaVersion persistedSchemaVersion;
    private final HikariDataSource sqliteDataSource;
    private final FeedbackHandler feedbackHandler;
    private final LeaderElectionMetricAggregator leaderElectionAggregator;
    private final TimeLockCorruptionComponents corruptionComponents;
    private final Runnable serviceStopper;
    private LeaderPingHealthCheck healthCheck;
    private TimelockNamespaces namespaces;

    public static TimeLockAgent create(
            MetricsManager metricsManager,
            TimeLockInstallConfiguration install,
            Refreshable<TimeLockRuntimeConfiguration> runtime,
            ClusterConfiguration cluster,
            UserAgent userAgent,
            int threadPoolSize,
            long blockingTimeoutMs,
            Consumer<Object> registrar,
            Optional<Consumer<UndertowService>> undertowRegistrar,
            OrderableSlsVersion timeLockVersion,
            ObjectMapper objectMapper,
            Runnable serviceStopper) {

        verifyConfigurationSanity(install, cluster);

        // Restricting access to user-provided runtime config
        Refreshable<TimeLockRuntimeConfiguration> restrictedRuntime =
                runtime.map(RestrictedTimeLockRuntimeConfiguration::new);

        TimeLockDialogueServiceProvider timeLockDialogueServiceProvider =
                createTimeLockDialogueServiceProvider(metricsManager, cluster, userAgent);
        PaxosResourcesFactory.TimelockPaxosInstallationContext installationContext =
                ImmutableTimelockPaxosInstallationContext.of(
                        install, cluster, userAgent, timeLockDialogueServiceProvider, timeLockVersion);

        // Upgrading the schema version should generally happen BEFORE any migration has started. Keep this in
        // mind for any potential live migrations
        PersistedSchemaVersion persistedSchemaVersion =
                PersistedSchemaVersion.create(installationContext.sqliteDataSource());
        persistedSchemaVersion.upgradeVersion(SCHEMA_VERSION);
        verifySchemaVersion(persistedSchemaVersion);

        verifyTimestampBoundPersisterConfiguration(
                installationContext.sqliteDataSource(),
                install.timestampBoundPersistence(),
                install.iAmOnThePersistenceTeamAndKnowWhatIAmDoingReseedPersistedPersisterConfiguration(),
                objectMapper);

        PaxosResources paxosResources = PaxosResourcesFactory.create(
                installationContext,
                metricsManager,
                Suppliers.compose(TimeLockRuntimeConfiguration::paxos, restrictedRuntime::get));

        TimeLockAgent agent = new TimeLockAgent(
                metricsManager,
                install,
                restrictedRuntime,
                cluster,
                undertowRegistrar,
                threadPoolSize,
                blockingTimeoutMs,
                registrar,
                paxosResources,
                userAgent,
                persistedSchemaVersion,
                installationContext.sqliteDataSource(),
                serviceStopper);
        agent.createAndRegisterResources();
        return agent;
    }

    private static void verifyConfigurationSanity(TimeLockInstallConfiguration install, ClusterConfiguration cluster) {
        verifyTopologyOffersHighAvailability(install, cluster);
        verifyIsNewServiceInvariant(install, cluster);
    }

    private static TimeLockDialogueServiceProvider createTimeLockDialogueServiceProvider(
            MetricsManager metricsManager, ClusterConfiguration cluster, UserAgent userAgent) {
        DialogueClients.ReloadingFactory baseFactory = DialogueClients.create(
                        Refreshable.only(ServicesConfigBlock.builder().build()))
                .withBlockingExecutor(PTExecutors.newCachedThreadPool("atlas-dialogue-blocking"));
        ServerListConfig timeLockServerListConfig = ImmutableServerListConfig.builder()
                .addAllServers(PaxosRemotingUtils.getRemoteServerPaths(cluster))
                .sslConfiguration(cluster.cluster().security())
                .proxyConfiguration(cluster.cluster().proxyConfiguration())
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

    private TimeLockAgent(
            MetricsManager metricsManager,
            TimeLockInstallConfiguration install,
            Refreshable<TimeLockRuntimeConfiguration> runtime,
            ClusterConfiguration cluster,
            Optional<Consumer<UndertowService>> undertowRegistrar,
            int threadPoolSize,
            long blockingTimeoutMs,
            Consumer<Object> registrar,
            PaxosResources paxosResources,
            UserAgent userAgent,
            PersistedSchemaVersion persistedSchemaVersion,
            HikariDataSource sqliteDataSource,
            Runnable serviceStopper) {
        this.metricsManager = metricsManager;
        this.install = install;
        this.runtime = runtime;
        this.cluster = cluster;
        this.undertowRegistrar = undertowRegistrar;
        this.registrar = registrar;
        this.paxosResources = paxosResources;
        this.sqliteDataSource = sqliteDataSource;
        this.serviceStopper = serviceStopper;
        this.lockCreator = new LockCreator(runtime, threadPoolSize, blockingTimeoutMs);
        this.timestampStorage = getTimestampStorage();
        this.persistedSchemaVersion = persistedSchemaVersion;

        LockLog lockLog = new LockLog(
                metricsManager.getRegistry(),
                Suppliers.compose(TimeLockRuntimeConfiguration::slowLockLogTriggerMillis, runtime::get));

        this.timelockCreator = new AsyncTimeLockServicesCreator(
                metricsManager, lockLog, paxosResources.leadershipComponents(), install.lockDiagnosticConfig());

        this.noSimultaneousServiceCheck = NoSimultaneousServiceCheck.create(
                new TimeLockActivityCheckerFactory(cluster, metricsManager, userAgent).getTimeLockActivityCheckers());

        this.feedbackHandler = new FeedbackHandler(
                metricsManager, () -> runtime.get().adjudication().enabled());
        this.corruptionComponents = paxosResources.timeLockCorruptionComponents();
        this.leaderElectionAggregator = new LeaderElectionMetricAggregator(metricsManager);
    }

    private TimestampStorage getTimestampStorage() {
        TsBoundPersisterConfiguration timestampBoundPersistence = install.timestampBoundPersistence();
        if (timestampBoundPersistence instanceof PaxosTsBoundPersisterConfiguration) {
            log.info("Starting TimeLock with Paxos timestamp persistence");
            return createPaxosBasedTimestampStorage();
        } else if (timestampBoundPersistence instanceof DatabaseTsBoundPersisterConfiguration) {
            log.info("Starting TimeLock with DB timestamp persistence");
            return createDatabaseTimestampStorage((DatabaseTsBoundPersisterConfiguration) timestampBoundPersistence);
        }
        throw new RuntimeException(
                String.format("Unknown TsBoundPersisterConfiguration found %s", timestampBoundPersistence.getClass()));
    }

    private TimestampStorage createPaxosBasedTimestampStorage() {
        return ImmutableTimestampStorage.builder()
                .timestampCreator(new PaxosTimestampCreator(paxosResources.timestampServiceFactory()))
                .persistentNamespaceContext(PersistentNamespaceContexts.timestampBoundPaxos(
                        install.paxos().dataDirectory().toPath(), sqliteDataSource))
                .build();
    }

    private TimestampStorage createDatabaseTimestampStorage(
            DatabaseTsBoundPersisterConfiguration timestampBoundPersistence) {
        ServiceDiscoveringDatabaseTimeLockSupplier dbTimeLockSupplier = new ServiceDiscoveringDatabaseTimeLockSupplier(
                metricsManager,
                timestampBoundPersistence.keyValueServiceConfig(),
                runtime.map(TimeLockAgent::getKeyValueServiceRuntimeConfig),
                createLeaderConfig());
        return ImmutableTimestampStorage.builder()
                .timestampCreator(new DbBoundTimestampCreator(dbTimeLockSupplier))
                .persistentNamespaceContext(PersistentNamespaceContexts.dbBound(
                        dbTimeLockSupplier.getTimestampSeriesProvider(AtlasDbConstants.DB_TIMELOCK_TIMESTAMP_TABLE)))
                .build();
    }

    @VisibleForTesting
    static Optional<KeyValueServiceRuntimeConfig> getKeyValueServiceRuntimeConfig(
            TimeLockRuntimeConfiguration timeLockRuntimeConfiguration) {
        return timeLockRuntimeConfiguration
                .timestampBoundPersistence()
                .map(config -> {
                    Preconditions.checkState(
                            config instanceof DatabaseTsBoundPersisterRuntimeConfiguration,
                            "Should not initialise DB Timelock with non-database runtime configuration");
                    return (DatabaseTsBoundPersisterRuntimeConfiguration) config;
                })
                .map(DatabaseTsBoundPersisterRuntimeConfiguration::keyValueServiceRuntimeConfig);
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

        Function<String, AsyncTimelockService> asyncTimelockServiceGetter =
                namespace -> namespaces.get(namespace).getTimelockService();
        Function<String, LockService> lockServiceGetter =
                namespace -> namespaces.get(namespace).getLockService();

        if (undertowRegistrar.isPresent()) {
            Consumer<UndertowService> presentUndertowRegistrar = undertowRegistrar.get();
            registerCorruptionHandlerWrappedService(
                    presentUndertowRegistrar,
                    ConjureTimelockResource.undertow(redirectRetryTargeter(), asyncTimelockServiceGetter));
            registerCorruptionHandlerWrappedService(
                    presentUndertowRegistrar,
                    ConjureLockWatchingResource.undertow(redirectRetryTargeter(), asyncTimelockServiceGetter));
            registerCorruptionHandlerWrappedService(
                    presentUndertowRegistrar,
                    ConjureLockV1Resource.undertow(redirectRetryTargeter(), lockServiceGetter));
            registerCorruptionHandlerWrappedService(
                    presentUndertowRegistrar,
                    TimeLockPaxosHistoryProviderResource.undertow(corruptionComponents.localHistoryLoader()));
            registerCorruptionHandlerWrappedService(
                    presentUndertowRegistrar,
                    MultiClientConjureTimelockResource.undertow(redirectRetryTargeter(), asyncTimelockServiceGetter));
        } else {
            registrar.accept(ConjureTimelockResource.jersey(redirectRetryTargeter(), asyncTimelockServiceGetter));
            registrar.accept(ConjureLockWatchingResource.jersey(redirectRetryTargeter(), asyncTimelockServiceGetter));
            registrar.accept(ConjureLockV1Resource.jersey(redirectRetryTargeter(), lockServiceGetter));
            registrar.accept(TimeLockPaxosHistoryProviderResource.jersey(corruptionComponents.localHistoryLoader()));
            registrar.accept(
                    MultiClientConjureTimelockResource.jersey(redirectRetryTargeter(), asyncTimelockServiceGetter));
        }
    }

    private void registerClientFeedbackService() {
        if (undertowRegistrar.isPresent()) {
            registerCorruptionHandlerWrappedService(
                    undertowRegistrar.get(),
                    TimeLockClientFeedbackResource.undertow(
                            feedbackHandler, this::isLeaderForClient, leaderElectionAggregator));
        } else {
            registrar.accept(TimeLockClientFeedbackResource.jersey(
                    feedbackHandler, this::isLeaderForClient, leaderElectionAggregator));
        }
    }

    private void registerTimeLockCorruptionNotifiers() {
        if (undertowRegistrar.isPresent()) {
            registerCorruptionHandlerWrappedService(
                    undertowRegistrar.get(),
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
        ServiceLifecycleController serviceLifecycleController =
                new ServiceLifecycleController(serviceStopper, PTExecutors.newSingleThreadScheduledExecutor());
        if (undertowRegistrar.isPresent()) {
            registerCorruptionHandlerWrappedService(
                    undertowRegistrar.get(),
                    TimeLockManagementResource.undertow(
                            timestampStorage.persistentNamespaceContext(),
                            namespaces,
                            redirectRetryTargeter(),
                            serviceLifecycleController));
        } else {
            registrar.accept(TimeLockManagementResource.jersey(
                    timestampStorage.persistentNamespaceContext(),
                    namespaces,
                    redirectRetryTargeter(),
                    serviceLifecycleController));
        }
    }

    private void registerTimeLockCorruptionJerseyFilter() {
        if (!undertowRegistrar.isPresent()) {
            registrar.accept(new JerseyCorruptionFilter(corruptionComponents.timeLockCorruptionHealthCheck()));
        }
    }

    private void registerCorruptionHandlerWrappedService(
            Consumer<UndertowService> presentUndertowRegistrar, UndertowService service) {
        presentUndertowRegistrar.accept(
                UndertowCorruptionHandlerService.of(service, corruptionComponents.timeLockCorruptionHealthCheck()));
    }

    @VisibleForTesting
    static void verifyIsNewServiceInvariant(TimeLockInstallConfiguration install, ClusterConfiguration cluster) {
        if (!install.paxos().ignoreNewServiceCheck()) {
            TimeLockPersistenceInvariants.checkPersistenceConsistentWithState(
                    install.isNewService() || cluster.isNewServiceNode(),
                    install.paxos().doDataDirectoriesExist());
        }
    }

    @VisibleForTesting
    static void verifyTopologyOffersHighAvailability(
            TimeLockInstallConfiguration install, ClusterConfiguration cluster) {
        if (install.cluster().enableNonstandardAndPossiblyDangerousTopology()
                || cluster.enableNonstandardAndPossiblyDangerousTopology()) {
            return;
        }

        Preconditions.checkArgument(
                cluster.clusterMembers().size() >= 3,
                "This TimeLock cluster is set up to use an insufficient (< 3) number of servers, which is not a"
                        + " standard configuration! With fewer than three servers, your service will not have high"
                        + " availability. In the event a node goes down, timelock will become unresponsive, meaning"
                        + " that ALL your AtlasDB clients will become unable to perform transactions. Furthermore, if"
                        + " 1-node, your TimeLock  cluster has NO resilience to failures of the underlying storage"
                        + " layer; if your disks fail, the timestamp information may be IRRECOVERABLY COMPROMISED,"
                        + " meaning that your AtlasDB deployments may become completely unusable."
                        + " If you know what you are doing and you want to run in this configuration, you must set"
                        + " 'enableNonstandardAndPossiblyDangerousTopology' to true.",
                SafeArg.of("clusterSize", cluster.clusterMembers().size()),
                SafeArg.of("minimumClusterSize", 3));
    }

    static void verifySchemaVersion(PersistedSchemaVersion persistedSchemaVersion) {
        Preconditions.checkState(
                persistedSchemaVersion.getVersion() == SCHEMA_VERSION,
                "Persisted schema version does not match timelock's current schema version.",
                SafeArg.of("current schema version", SCHEMA_VERSION),
                SafeArg.of("persisted schema version", persistedSchemaVersion.getVersion()));
    }

    @VisibleForTesting
    static void verifyTimestampBoundPersisterConfiguration(
            HikariDataSource sqliteDataSource,
            TsBoundPersisterConfiguration currentUserConfiguration,
            boolean reseedPersistedPersisterConfiguration,
            ObjectMapper objectMapper) {
        PersistenceConfigStore store =
                new PersistenceConfigStore(objectMapper, SqliteBlobStore.create(sqliteDataSource));
        if (reseedPersistedPersisterConfiguration) {
            log.info(
                    "As configured, updating the configuration persisted in the SQLite database.",
                    SafeArg.of("ourConfiguration", currentUserConfiguration));
            store.storeConfig(currentUserConfiguration);
            return;
        }

        Optional<TsBoundPersisterConfiguration> configInDatabase = store.getPersistedConfig();

        if (!configInDatabase.isPresent()) {
            log.info(
                    "There is no config in the SQLite database indicating where timestamps are being stored. We are"
                            + " thus assuming that your current configuration is indeed correct, and using that as a"
                            + " future reference.",
                    SafeArg.of("configuration", currentUserConfiguration));
            store.storeConfig(currentUserConfiguration);
            return;
        }

        TsBoundPersisterConfiguration presentConfig = configInDatabase.get();
        if (currentUserConfiguration.isLocationallyIncompatible(presentConfig)) {
            log.error(
                    "Configuration in the SQLite database does not agree with what the user has provided!",
                    SafeArg.of("ourConfiguration", currentUserConfiguration),
                    SafeArg.of("persistedConfiguration", presentConfig));
            throw new SafeIllegalStateException("Configuration in the SQLite database does not agree with the"
                    + " configuration the user has provided, in a way that is known to be incompatible. For integrity"
                    + " of the service, we will shut down and cannot serve any user requests. If you have"
                    + " accidentally changed the DB configs, please revert them. If this is intentional, you can"
                    + " update the config stored in the database by setting the relevant override flag.");
        } else {
            log.info("Passed consistency check: the config in the SQLite database agrees with our config.");
        }
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
        return persistedSchemaVersion.getVersion();
    }

    @SuppressWarnings("unused")
    public long getLatestSchemaVersion() {
        return SCHEMA_VERSION;
    }

    // No runtime configuration at the moment.
    private void registerPaxosResource() {
        paxosResources.resourcesForRegistration().forEach(registrar);
    }

    private void registerExceptionMappers() {
        registrar.accept(new BlockingTimeoutExceptionMapper());

        registrar.accept(new NotCurrentLeaderExceptionMapper(redirectRetryTargeter()));

        registrar.accept(new TooManyRequestsExceptionMapper());
    }

    private RedirectRetryTargeter redirectRetryTargeter() {
        URL localServer = PaxosRemotingUtils.convertAddressToUrl(cluster, cluster.localServer());
        List<URL> clusterUrls = PaxosRemotingUtils.convertAddressesToUrls(cluster, cluster.clusterMembers());
        return RedirectRetryTargeter.create(localServer, clusterUrls);
    }

    /**
     * Creates timestamp and lock services for the given client. It is expected that for each client there should
     * only be (up to) one active timestamp service, and one active lock service at any time.
     * @param client Client namespace to create the services for
     * @return Invalidating timestamp and lock services
     */
    public TimeLockServices createInvalidatingTimeLockServices(String client) {
        LeaderConfig leaderConfig = createLeaderConfig();

        Client typedClient = Client.of(client);

        Supplier<ManagedTimestampService> rawTimestampServiceSupplier =
                timestampStorage.timestampCreator().createTimestampService(typedClient, leaderConfig);
        Supplier<LockService> rawLockServiceSupplier = lockCreator::createThreadPoolingLockService;
        return timelockCreator.createTimeLockServices(typedClient, rawTimestampServiceSupplier, rawLockServiceSupplier);
    }

    private LeaderConfig createLeaderConfig() {
        List<String> uris = cluster.clusterMembers();
        Path leaderPaxosDataDirectory = install.paxos().dataDirectory().toPath().resolve(LEADER_PAXOS_NAMESPACE);

        return ImmutableLeaderConfig.builder()
                .addLeaders(uris.toArray(new String[0]))
                .localServer(cluster.localServer())
                .sslConfiguration(PaxosRemotingUtils.getSslConfigurationOptional(cluster))
                .quorumSize(PaxosRemotingUtils.getQuorumSize(uris))
                .learnerLogDir(leaderPaxosDataDirectory
                        .resolve(LEARNER_SUBDIRECTORY_PATH)
                        .toFile())
                .acceptorLogDir(leaderPaxosDataDirectory
                        .resolve(ACCEPTOR_SUBDIRECTORY_PATH)
                        .toFile())
                .build();
    }

    public HealthStatusReport timeLockAdjudicationFeedback() {
        return feedbackHandler.getTimeLockHealthStatus();
    }

    public LeaderElectionHealthReport timeLockLeadershipHealthCheck() {
        return paxosResources
                .leadershipContextFactory()
                .leaderElectionHealthCheck()
                .leaderElectionRateHealthReport();
    }

    public CorruptionHealthReport timeLockCorruptionHealthCheck() {
        return corruptionComponents.timeLockCorruptionHealthCheck().localCorruptionReport();
    }

    public void shutdown() {
        paxosResources.leadershipComponents().shutdown();
        timestampStorage.close();
        sqliteDataSource.close();
    }
}
