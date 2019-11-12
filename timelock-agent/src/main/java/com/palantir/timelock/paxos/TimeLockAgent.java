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
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
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
import com.palantir.atlasdb.timelock.TimeLockResource;
import com.palantir.atlasdb.timelock.TimeLockServices;
import com.palantir.atlasdb.timelock.TooManyRequestsExceptionMapper;
import com.palantir.atlasdb.timelock.config.TargetedSweepLockControlConfig;
import com.palantir.atlasdb.timelock.lock.LockLog;
import com.palantir.atlasdb.timelock.paxos.Client;
import com.palantir.atlasdb.timelock.paxos.ImmutableTimelockPaxosInstallationContext;
import com.palantir.atlasdb.timelock.paxos.PaxosResources;
import com.palantir.atlasdb.timelock.paxos.PaxosResourcesFactory;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.leader.PaxosLeaderElectionService;
import com.palantir.lock.LockService;
import com.palantir.timelock.TimeLockStatus;
import com.palantir.timelock.config.DatabaseTsBoundPersisterConfiguration;
import com.palantir.timelock.config.PaxosTsBoundPersisterConfiguration;
import com.palantir.timelock.config.TimeLockInstallConfiguration;
import com.palantir.timelock.config.TimeLockRuntimeConfiguration;
import com.palantir.timelock.config.TsBoundPersisterConfiguration;
import com.palantir.timestamp.ManagedTimestampService;

@SuppressWarnings("checkstyle:FinalClass") // This is mocked internally
public class TimeLockAgent {
    private static final Long SCHEMA_VERSION = 1L;

    private final MetricsManager metricsManager;
    private final TimeLockInstallConfiguration install;
    private final Supplier<TimeLockRuntimeConfiguration> runtime;
    private final Consumer<Object> registrar;
    private final PaxosResources paxosResources;
    private final LockCreator lockCreator;
    private final TimestampCreator timestampCreator;
    private final TimeLockServicesCreator timelockCreator;

    private LeaderPingHealthCheck healthCheck;
    private TimeLockResource resource;

    public static TimeLockAgent create(
            MetricsManager metricsManager,
            TimeLockInstallConfiguration install,
            Supplier<TimeLockRuntimeConfiguration> runtime,
            int threadPoolSize,
            long blockingTimeoutMs,
            Consumer<Object> registrar) {
        ExecutorService executor = createSharedExecutor(metricsManager);
        PaxosResources paxosResources = PaxosResourcesFactory.create(
                ImmutableTimelockPaxosInstallationContext.of(install),
                metricsManager,
                Suppliers.compose(TimeLockRuntimeConfiguration::paxos, runtime::get),
                executor);

        TimeLockAgent agent = new TimeLockAgent(
                metricsManager,
                install,
                runtime,
                threadPoolSize,
                blockingTimeoutMs,
                registrar,
                paxosResources);
        agent.createAndRegisterResources();
        return agent;
    }

    private TimeLockAgent(MetricsManager metricsManager,
            TimeLockInstallConfiguration install,
            Supplier<TimeLockRuntimeConfiguration> runtime,
            int threadPoolSize,
            long blockingTimeoutMs,
            Consumer<Object> registrar,
            PaxosResources paxosResources) {
        this.metricsManager = metricsManager;
        this.install = install;
        this.runtime = runtime;
        this.registrar = registrar;
        this.paxosResources = paxosResources;
        this.lockCreator = new LockCreator(runtime, threadPoolSize, blockingTimeoutMs);
        this.timestampCreator = getTimestampCreator();
        LockLog lockLog = new LockLog(metricsManager.getRegistry(),
                Suppliers.compose(TimeLockRuntimeConfiguration::slowLockLogTriggerMillis, runtime::get));

        Supplier<TargetedSweepLockControlConfig> targetedSweepLockControlConfig = Suppliers.compose(
                TimeLockRuntimeConfiguration::targetedSweepLockControlConfig, runtime::get);
        this.timelockCreator = new AsyncTimeLockServicesCreator(
                metricsManager,
                lockLog,
                paxosResources.leadershipComponents(),
                install.lockDiagnosticConfig(),
                targetedSweepLockControlConfig);
    }

    private static ExecutorService createSharedExecutor(MetricsManager metricsManager) {
        return new InstrumentedExecutorService(
                PTExecutors.newCachedThreadPool(
                        new InstrumentedThreadFactory(new ThreadFactoryBuilder()
                                .setNameFormat("paxos-timestamp-creator-%d")
                                .setDaemon(true)
                                .build(), metricsManager.getRegistry())),
                metricsManager.getRegistry(),
                MetricRegistry.name(PaxosLeaderElectionService.class, "paxos-timestamp-creator", "executor"));
    }

    private TimestampCreator getTimestampCreator() {
        TsBoundPersisterConfiguration timestampBoundPersistence = install.timestampBoundPersistence();
        if (timestampBoundPersistence instanceof PaxosTsBoundPersisterConfiguration) {
            return getPaxosTimestampCreator();
        } else if (timestampBoundPersistence instanceof DatabaseTsBoundPersisterConfiguration) {
            return new DbBoundTimestampCreator(
                    ((DatabaseTsBoundPersisterConfiguration) timestampBoundPersistence)
                            .keyValueServiceConfig());
        }
        throw new RuntimeException(String.format("Unknown TsBoundPersisterConfiguration found %s",
                timestampBoundPersistence.getClass()));
    }

    private PaxosTimestampCreator getPaxosTimestampCreator() {
        return new PaxosTimestampCreator(
                paxosResources.timestamp(),
                Suppliers.compose(TimeLockRuntimeConfiguration::paxos, runtime::get));
    }

    private void createAndRegisterResources() {
        registerPaxosResource();
        registerExceptionMappers();

        // Finally, register the health check, and endpoints associated with the clients.
        healthCheck = paxosResources.leadershipComponents().healthCheck();
        resource = TimeLockResource.create(
                metricsManager,
                this::createInvalidatingTimeLockServices,
                Suppliers.compose(TimeLockRuntimeConfiguration::maxNumberOfClients, runtime::get));
        registrar.accept(resource);
    }

    @SuppressWarnings("unused") // used by external health checks
    public TimeLockStatus getStatus() {
        if (getNumberOfActiveClients() == 0) {
            return TimeLockStatus.PENDING_ELECTION;
        }

        return healthCheck.getStatus();
    }

    @SuppressWarnings({"unused", "WeakerAccess"}) // used by external health checks
    public int getNumberOfActiveClients() {
        return resource.getNumberOfActiveClients();
    }

    @SuppressWarnings("unused") // used by external health checks
    public int getMaxNumberOfClients() {
        return resource.getMaxNumberOfClients();
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

        URL localServer = PaxosRemotingUtils.convertAddressToUrl(install, install.cluster().localServer());
        List<URL> clusterUrls = PaxosRemotingUtils.convertAddressesToUrls(install, install.cluster().clusterMembers());
        registrar.accept(new NotCurrentLeaderExceptionMapper(RedirectRetryTargeter.create(localServer, clusterUrls)));

        registrar.accept(new TooManyRequestsExceptionMapper());
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
