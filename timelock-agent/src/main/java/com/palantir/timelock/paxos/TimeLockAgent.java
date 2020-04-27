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
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import com.codahale.metrics.InstrumentedExecutorService;
import com.codahale.metrics.InstrumentedThreadFactory;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.palantir.atlasdb.config.ImmutableLeaderConfig;
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
import com.palantir.atlasdb.timelock.lock.LockLog;
import com.palantir.atlasdb.timelock.management.TimeLockManagementResource;
import com.palantir.atlasdb.timelock.paxos.Client;
import com.palantir.atlasdb.timelock.paxos.ImmutableTimelockPaxosInstallationContext;
import com.palantir.atlasdb.timelock.paxos.PaxosResources;
import com.palantir.atlasdb.timelock.paxos.PaxosResourcesFactory;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.undertow.lib.UndertowService;
import com.palantir.leader.PaxosLeaderElectionService;
import com.palantir.lock.LockService;
import com.palantir.timelock.config.DatabaseTsBoundPersisterConfiguration;
import com.palantir.timelock.config.PaxosTsBoundPersisterConfiguration;
import com.palantir.timelock.config.TimeLockInstallConfiguration;
import com.palantir.timelock.config.TimeLockRuntimeConfiguration;
import com.palantir.timelock.config.TsBoundPersisterConfiguration;
import com.palantir.timelock.invariants.NoSimultaneousServiceCheck;
import com.palantir.timelock.invariants.TimeLockActivityCheckerFactory;
import com.palantir.timestamp.ManagedTimestampService;

@SuppressWarnings("checkstyle:FinalClass") // This is mocked internally
public class TimeLockAgent {
    private static final Long SCHEMA_VERSION = 1L;

    private static final int MAX_SHARED_EXECUTOR_THREADS = 256;
    private static final int CORE_SHARED_EXECUTOR_THREADS = 10;
    private static final String PAXOS_SHARED_EXECUTOR = "paxos-shared-executor";

    private final MetricsManager metricsManager;
    private final TimeLockInstallConfiguration install;
    private final Supplier<TimeLockRuntimeConfiguration> runtime;
    private final Consumer<Object> registrar;
    private final Optional<Consumer<UndertowService>> undertowRegistrar;
    private final PaxosResources paxosResources;
    private final LockCreator lockCreator;
    private final TimestampCreator timestampCreator;
    private final TimeLockServicesCreator timelockCreator;
    private final NoSimultaneousServiceCheck noSimultaneousServiceCheck;

    private LeaderPingHealthCheck healthCheck;
    private TimelockNamespaces namespaces;

    public static TimeLockAgent create(
            MetricsManager metricsManager,
            TimeLockInstallConfiguration install,
            Supplier<TimeLockRuntimeConfiguration> runtime,
            UserAgent userAgent,
            int threadPoolSize,
            long blockingTimeoutMs,
            Consumer<Object> registrar,
            Optional<Consumer<UndertowService>> undertowRegistrar) {
        ExecutorService executor = createSharedExecutor(metricsManager);
        PaxosResources paxosResources = PaxosResourcesFactory.create(
                ImmutableTimelockPaxosInstallationContext.of(install, userAgent),
                metricsManager,
                Suppliers.compose(TimeLockRuntimeConfiguration::paxos, runtime::get),
                executor);

        TimeLockAgent agent = new TimeLockAgent(
                metricsManager,
                install,
                runtime,
                undertowRegistrar,
                threadPoolSize,
                blockingTimeoutMs,
                registrar,
                paxosResources,
                userAgent);
        agent.createAndRegisterResources();
        return agent;
    }

    private TimeLockAgent(MetricsManager metricsManager,
            TimeLockInstallConfiguration install,
            Supplier<TimeLockRuntimeConfiguration> runtime,
            Optional<Consumer<UndertowService>> undertowRegistrar,
            int threadPoolSize,
            long blockingTimeoutMs,
            Consumer<Object> registrar,
            PaxosResources paxosResources,
            UserAgent userAgent) {
        this.metricsManager = metricsManager;
        this.install = install;
        this.runtime = runtime;
        this.undertowRegistrar = undertowRegistrar;
        this.registrar = registrar;
        this.paxosResources = paxosResources;
        this.lockCreator = new LockCreator(runtime, threadPoolSize, blockingTimeoutMs);
        this.timestampCreator = getTimestampCreator();
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
    }

    private static ExecutorService createSharedExecutor(MetricsManager metricsManager) {
        return new InstrumentedExecutorService(
                PTExecutors.newCachedThreadPool(new InstrumentedThreadFactory(new ThreadFactoryBuilder()
                        .setNameFormat("paxos-timestamp-creator-%d")
                        .setDaemon(true)
                        .build(), metricsManager.getRegistry())),
                metricsManager.getRegistry(),
                MetricRegistry.name(PaxosLeaderElectionService.class, PAXOS_SHARED_EXECUTOR, "executor"));
    }

    private TimestampCreator getTimestampCreator() {
        TsBoundPersisterConfiguration timestampBoundPersistence = install.timestampBoundPersistence();
        if (timestampBoundPersistence instanceof PaxosTsBoundPersisterConfiguration) {
            return new PaxosTimestampCreator(paxosResources.timestampServiceFactory());
        } else if (timestampBoundPersistence instanceof DatabaseTsBoundPersisterConfiguration) {
            return new DbBoundTimestampCreator(
                    ((DatabaseTsBoundPersisterConfiguration) timestampBoundPersistence)
                            .keyValueServiceConfig());
        }
        throw new RuntimeException(String.format("Unknown TsBoundPersisterConfiguration found %s",
                timestampBoundPersistence.getClass()));
    }

    private void createAndRegisterResources() {
        registerPaxosResource();
        registerExceptionMappers();
        registerManagementResource();

        namespaces = new TimelockNamespaces(
                metricsManager,
                this::createInvalidatingTimeLockServices,
                Suppliers.compose(TimeLockRuntimeConfiguration::maxNumberOfClients, runtime::get));

        // Finally, register the health check, and endpoints associated with the clients.
        TimeLockResource resource = TimeLockResource.create(namespaces);
        healthCheck = paxosResources.leadershipComponents().healthCheck(namespaces::getActiveClients);

        registrar.accept(resource);

        Function<String, AsyncTimelockService> creator = namespace -> namespaces.get(namespace).getTimelockService();
        if (undertowRegistrar.isPresent()) {
            undertowRegistrar.get().accept(ConjureTimelockResource.undertow(redirectRetryTargeter(), creator));
            undertowRegistrar.get().accept(ConjureLockWatchingResource.undertow(redirectRetryTargeter(), creator));
        } else {
            registrar.accept(ConjureTimelockResource.jersey(redirectRetryTargeter(), creator));
            registrar.accept(ConjureLockWatchingResource.jersey(redirectRetryTargeter(), creator));
        }
    }

    private void registerManagementResource() {
        Path rootDataDirectory = install.paxos().dataDirectory().toPath();
        if (undertowRegistrar.isPresent()) {
            undertowRegistrar.get().accept(TimeLockManagementResource.undertow(rootDataDirectory));
        } else {
            registrar.accept(TimeLockManagementResource.jersey(rootDataDirectory));
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
        List<String> uris = install.cluster().clusterMembers();
        ImmutableLeaderConfig leaderConfig = ImmutableLeaderConfig.builder()
                .addLeaders(uris.toArray(new String[0]))
                .localServer(install.cluster().localServer())
                .sslConfiguration(PaxosRemotingUtils.getSslConfigurationOptional(install))
                .quorumSize(PaxosRemotingUtils.getQuorumSize(uris))
                .build();

        Client typedClient = Client.of(client);
        Supplier<ManagedTimestampService> rawTimestampServiceSupplier = timestampCreator
                .createTimestampService(typedClient, leaderConfig);
        Supplier<LockService> rawLockServiceSupplier = lockCreator::createThreadPoolingLockService;
        return timelockCreator.createTimeLockServices(typedClient, rawTimestampServiceSupplier, rawLockServiceSupplier);
    }

    public void shutdown() {
        paxosResources.leadershipComponents().shutdown();
    }
}
