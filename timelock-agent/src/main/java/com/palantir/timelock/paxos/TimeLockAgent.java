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

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.codahale.metrics.InstrumentedExecutorService;
import com.codahale.metrics.InstrumentedThreadFactory;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.palantir.atlasdb.config.ImmutableLeaderConfig;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.http.BlockingTimeoutExceptionMapper;
import com.palantir.atlasdb.http.NotCurrentLeaderExceptionMapper;
import com.palantir.atlasdb.timelock.TimeLockResource;
import com.palantir.atlasdb.timelock.TimeLockServices;
import com.palantir.atlasdb.timelock.TooManyRequestsExceptionMapper;
import com.palantir.atlasdb.timelock.lock.LockLog;
import com.palantir.atlasdb.timelock.paxos.PaxosResource;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.conjure.java.config.ssl.SslSocketFactories;
import com.palantir.conjure.java.config.ssl.TrustContext;
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

    private final PaxosResource paxosResource;
    private final PaxosLeadershipCreator leadershipCreator;
    private final LockCreator lockCreator;
    private final TimestampCreator timestampCreator;
    private final TimeLockServicesCreator timelockCreator;
    private final ExecutorService sharedExecutor;

    private Supplier<LeaderPingHealthCheck> healthCheckSupplier;
    private TimeLockResource resource;

    public static TimeLockAgent create(
            MetricsManager metricsManager,
            TimeLockInstallConfiguration install,
            Supplier<TimeLockRuntimeConfiguration> runtime,
            int threadPoolSize,
            long blockingTimeoutMs,
            Consumer<Object> registrar) {
        ExecutorService executor = createSharedExecutor(metricsManager);
        TimeLockAgent agent = new TimeLockAgent(metricsManager, install, runtime, threadPoolSize, blockingTimeoutMs,
                registrar, executor);
        agent.createAndRegisterResources();
        return agent;
    }

    private TimeLockAgent(MetricsManager metricsManager,
            TimeLockInstallConfiguration install,
            Supplier<TimeLockRuntimeConfiguration> runtime,
            int threadPoolSize,
            long blockingTimeoutMs,
            Consumer<Object> registrar,
            ExecutorService sharedExecutor) {
        this.metricsManager = metricsManager;
        this.install = install;
        this.runtime = runtime;
        this.registrar = registrar;
        this.sharedExecutor = sharedExecutor;
        this.paxosResource = PaxosResource.create(metricsManager.getRegistry(),
                install.paxos().dataDirectory().toString());
        this.lockCreator = new LockCreator(runtime, threadPoolSize, blockingTimeoutMs);
        this.leadershipCreator = new PaxosLeadershipCreator(metricsManager, install, runtime, registrar);
        this.timestampCreator = getTimestampCreator(metricsManager.getRegistry());
        LockLog lockLog = new LockLog(metricsManager.getRegistry(),
                Suppliers.compose(TimeLockRuntimeConfiguration::slowLockLogTriggerMillis, runtime::get));
        this.timelockCreator = new AsyncTimeLockServicesCreator(metricsManager, lockLog, leadershipCreator);
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

    private TimestampCreator getTimestampCreator(MetricRegistry metrics) {
        TsBoundPersisterConfiguration timestampBoundPersistence = install.timestampBoundPersistence();
        if (timestampBoundPersistence instanceof PaxosTsBoundPersisterConfiguration) {
            return getPaxosTimestampCreator(metrics);
        } else if (timestampBoundPersistence instanceof DatabaseTsBoundPersisterConfiguration) {
            return new DbBoundTimestampCreator(
                    ((DatabaseTsBoundPersisterConfiguration) timestampBoundPersistence)
                            .keyValueServiceConfig());
        }
        throw new RuntimeException(String.format("Unknown TsBoundPersisterConfiguration found %s",
                timestampBoundPersistence.getClass()));
    }

    private PaxosTimestampCreator getPaxosTimestampCreator(MetricRegistry metrics) {
        List<ClientAwarePaxosAcceptor> paxosAcceptors = createProxies(ClientAwarePaxosAcceptor.class,
                "timestamp-bound-store.acceptor");
        List<ClientAwarePaxosLearner> paxosLearners = createProxies(ClientAwarePaxosLearner.class,
                "timestamp-bound-store.learner");
        return new PaxosTimestampCreator(
                metrics,
                paxosResource,
                Suppliers.compose(TimeLockRuntimeConfiguration::paxos, runtime::get),
                ClientAwarePaxosAcceptorAdapter.wrap(paxosAcceptors),
                ClientAwarePaxosLearnerAdapter.wrap(paxosLearners),
                sharedExecutor);
    }

    private <T> List<T> createProxies(Class<T> clazz, String userAgent) {
        Set<String> remoteUris = PaxosRemotingUtils.getRemoteServerPaths(install);
        Optional<TrustContext> trustContext = PaxosRemotingUtils.getSslConfigurationOptional(install)
                .map(SslSocketFactories::createTrustContext);
        return remoteUris.stream()
                .map(uri -> AtlasDbHttpClients.createProxyWithoutRetrying(
                        metricsManager.getRegistry(),
                        trustContext,
                        uri,
                        clazz,
                        userAgent,
                        false))
                .collect(Collectors.toList());
    }

    private void createAndRegisterResources() {
        registerPaxosResource();
        registerExceptionMappers();
        leadershipCreator.registerLeaderElectionService();

        // Finally, register the health check, and endpoints associated with the clients.
        healthCheckSupplier = leadershipCreator.getHealthCheck();
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

        return healthCheckSupplier.get().getStatus();
    }

    @SuppressWarnings("unused") // used by external health checks
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
        registrar.accept(paxosResource);
    }

    private void registerExceptionMappers() {
        registrar.accept(new BlockingTimeoutExceptionMapper());
        registrar.accept(new NotCurrentLeaderExceptionMapper());
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
                .addLeaders(uris.toArray(new String[uris.size()]))
                .localServer(install.cluster().localServer())
                .sslConfiguration(PaxosRemotingUtils.getSslConfigurationOptional(install))
                .quorumSize(PaxosRemotingUtils.getQuorumSize(uris))
                .build();

        Supplier<ManagedTimestampService> rawTimestampServiceSupplier = timestampCreator
                .createTimestampService(client, leaderConfig);
        Supplier<LockService> rawLockServiceSupplier = lockCreator::createThreadPoolingLockService;
        return timelockCreator.createTimeLockServices(client, rawTimestampServiceSupplier, rawLockServiceSupplier);
    }
}
