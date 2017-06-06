/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.timelock.paxos;

import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

import javax.net.ssl.SSLSocketFactory;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.palantir.atlasdb.config.ImmutableLeaderConfig;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.factory.ImmutableRemotePaxosServerSpec;
import com.palantir.atlasdb.factory.Leaders;
import com.palantir.atlasdb.factory.ServiceCreator;
import com.palantir.atlasdb.http.BlockingTimeoutExceptionMapper;
import com.palantir.atlasdb.http.NotCurrentLeaderExceptionMapper;
import com.palantir.atlasdb.timelock.TimeLockServer;
import com.palantir.atlasdb.timelock.TimeLockServices;
import com.palantir.atlasdb.timelock.TooManyRequestsExceptionMapper;
import com.palantir.atlasdb.timelock.config.PaxosConfiguration;
import com.palantir.atlasdb.timelock.config.TimeLockServerConfiguration;
import com.palantir.atlasdb.timelock.lock.BlockingTimeLimitedLockService;
import com.palantir.atlasdb.timelock.lock.BlockingTimeouts;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.leader.LeaderElectionService;
import com.palantir.leader.PingableLeader;
import com.palantir.leader.proxy.AwaitingLeadershipProxy;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.LockService;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.lock.impl.ThreadPooledLockService;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosProposer;
import com.palantir.paxos.PaxosProposerImpl;
import com.palantir.remoting2.config.ssl.SslSocketFactories;
import com.palantir.timestamp.PersistentTimestampService;
import com.palantir.timestamp.TimestampBoundStore;

import io.dropwizard.setup.Environment;

public class PaxosTimeLockServer implements TimeLockServer {
    private final PaxosConfiguration paxosConfiguration;
    private final Environment environment;

    private Set<String> remoteServers;
    private Optional<SSLSocketFactory> optionalSecurity = Optional.absent();
    private LeaderElectionService leaderElectionService;
    private PaxosResource paxosResource;
    private Semaphore sharedThreadPool = new Semaphore(-1);
    private TimeLockServerConfiguration timeLockServerConfiguration;

    public PaxosTimeLockServer(PaxosConfiguration configuration, Environment environment) {
        this.paxosConfiguration = configuration;
        this.environment = environment;
    }

    @Override
    public void onStartup(TimeLockServerConfiguration configuration) {
        registerPaxosResource();

        optionalSecurity = constructOptionalSslSocketFactory(paxosConfiguration);
        timeLockServerConfiguration = configuration;
        registerExceptionMappers();

        registerLeaderElectionService(configuration);

        registerHealthCheck(configuration);
    }

    private void registerExceptionMappers() {
        if (timeLockServerConfiguration.useClientRequestLimit()) {
            environment.jersey().register(new TooManyRequestsExceptionMapper());
        }
        environment.jersey().register(new BlockingTimeoutExceptionMapper());
        environment.jersey().register(new NotCurrentLeaderExceptionMapper());
    }

    private void registerPaxosResource() {
        paxosResource = PaxosResource.create(paxosConfiguration.paxosDataDir().toString());
        environment.jersey().register(paxosResource);
    }

    private void registerLeaderElectionService(TimeLockServerConfiguration configuration) {
        remoteServers = getRemoteServerPaths(configuration);

        LeaderConfig leaderConfig = getLeaderConfig(configuration);

        Set<String> paxosSubresourceUris = PaxosTimeLockUriUtils.getLeaderPaxosUris(remoteServers);

        Leaders.LocalPaxosServices localPaxosServices = Leaders.createInstrumentedLocalServices(
                leaderConfig,
                ImmutableRemotePaxosServerSpec.builder()
                        .remoteLeaderUris(remoteServers)
                        .remoteAcceptorUris(paxosSubresourceUris)
                        .remoteLearnerUris(paxosSubresourceUris)
                        .build(),
                "leader-election-service");
        leaderElectionService = localPaxosServices.leaderElectionService();

        environment.jersey().register(localPaxosServices.pingableLeader());
        environment.jersey().register(new LeadershipResource(
                localPaxosServices.ourAcceptor(),
                localPaxosServices.ourLearner()));
    }

    private void registerHealthCheck(TimeLockServerConfiguration configuration) {
        Set<PingableLeader> pingableLeaders = Leaders.generatePingables(
                getAllServerPaths(configuration),
                ServiceCreator.createSslSocketFactory(paxosConfiguration.sslConfiguration()),
                "leader-ping-healthcheck").keySet();
        environment.healthChecks().register("leader-ping", new LeaderPingHealthCheck(pingableLeaders));
    }

    private LeaderConfig getLeaderConfig(TimeLockServerConfiguration configuration) {
        return ImmutableLeaderConfig.builder()
                    .sslConfiguration(paxosConfiguration.sslConfiguration())
                    .leaders(addProtocols(configuration.cluster().servers()))
                    .localServer(addProtocol(configuration.cluster().localServer()))
                    .acceptorLogDir(Paths.get(paxosConfiguration.paxosDataDir().toString(),
                            PaxosTimeLockConstants.LEADER_PAXOS_NAMESPACE,
                            PaxosTimeLockConstants.ACCEPTOR_SUBDIRECTORY_PATH).toFile())
                    .learnerLogDir(Paths.get(paxosConfiguration.paxosDataDir().toString(),
                            PaxosTimeLockConstants.LEADER_PAXOS_NAMESPACE,
                            PaxosTimeLockConstants.LEARNER_SUBDIRECTORY_PATH).toFile())
                    .pingRateMs(paxosConfiguration.pingRateMs())
                    .quorumSize(getQuorumSize(configuration.cluster().servers()))
                    .leaderPingResponseWaitMs(paxosConfiguration.leaderPingResponseWaitMs())
                    .randomWaitBeforeProposingLeadershipMs(paxosConfiguration.maximumWaitBeforeProposalMs())
                    .build();
    }

    @VisibleForTesting
    static <T> int getQuorumSize(Collection<T> elements) {
        return elements.size() / 2 + 1;
    }

    private static Optional<SSLSocketFactory> constructOptionalSslSocketFactory(
            PaxosConfiguration configuration) {
        return configuration.sslConfiguration().transform(SslSocketFactories::createSslSocketFactory);
    }

    @Override
    public TimeLockServices createInvalidatingTimeLockServices(String client, long slowLogTriggerMillis) {
        ManagedTimestampService timestampService = instrument(
                ManagedTimestampService.class,
                createPaxosBackedTimestampService(client),
                client);
        LockService lockService = instrument(
                LockService.class,
                createLockService(slowLogTriggerMillis),
                client);

        return TimeLockServices.create(timestampService, lockService, timestampService);
    }

    private LockService createLockService(long slowLogTriggerMillis) {
        return AwaitingLeadershipProxy.newProxyInstance(
                LockService.class,
                () -> createThreadPoolingLockService(slowLogTriggerMillis),
                leaderElectionService);
    }

    private LockService createThreadPoolingLockService(long slowLogTriggerMillis) {
        LockService lockServiceNotUsingThreadPooling = createTimeLimitedLockService(slowLogTriggerMillis);

        if (!timeLockServerConfiguration.useClientRequestLimit()) {
            return lockServiceNotUsingThreadPooling;
        }

        int availableThreads = timeLockServerConfiguration.availableThreads();
        int numClients = timeLockServerConfiguration.clients().size();
        int localThreadPoolSize = (availableThreads / numClients) / 2;
        int sharedThreadPoolSize = availableThreads - localThreadPoolSize * numClients;

        // TODO a more robust solution is needed for live reloading -- probably we can take the delegate and rewrap it
        synchronized (this) {
            if (sharedThreadPool.availablePermits() == -1) {
                sharedThreadPool.release(sharedThreadPoolSize + 1);
            }
        }

        return new ThreadPooledLockService(lockServiceNotUsingThreadPooling, localThreadPoolSize, sharedThreadPool);
    }

    private LockService createTimeLimitedLockService(long slowLogTriggerMillis) {
        LockServerOptions lockServerOptions = new LockServerOptions() {
            @Override
            public long slowLogTriggerMillis() {
                return slowLogTriggerMillis;
            }
        };

        LockService rawLockService = LockServiceImpl.create(lockServerOptions);

        if (timeLockServerConfiguration.timeLimiterConfiguration().enableTimeLimiting()) {
            return BlockingTimeLimitedLockService.create(
                    rawLockService,
                    BlockingTimeouts.getBlockingTimeout(environment.getObjectMapper(), timeLockServerConfiguration));
        }
        return rawLockService;
    }

    private static <T> T instrument(Class<T> serviceClass, T service, String client) {
        return AtlasDbMetrics.instrument(serviceClass, service, MetricRegistry.name(serviceClass, client));
    }

    private ManagedTimestampService createPaxosBackedTimestampService(String client) {
        paxosResource.addInstrumentedClient(client);

        ExecutorService executor = Executors.newCachedThreadPool(new ThreadFactoryBuilder()
                .setNameFormat("atlas-consensus-" + client + "-%d")
                .setDaemon(true)
                .build());

        Set<String> namespacedUris = PaxosTimeLockUriUtils.getClientPaxosUris(remoteServers, client);
        List<PaxosAcceptor> acceptors = Leaders.createProxyAndLocalList(
                paxosResource.getPaxosAcceptor(client),
                namespacedUris,
                optionalSecurity,
                PaxosAcceptor.class,
                "timestamp-bound-store." + client);

        PaxosLearner ourLearner = paxosResource.getPaxosLearner(client);
        List<PaxosLearner> learners = Leaders.createProxyAndLocalList(
                ourLearner,
                namespacedUris,
                optionalSecurity,
                PaxosLearner.class,
                "timestamp-bound-store." + client);

        PaxosProposer proposer = instrument(PaxosProposer.class,
                PaxosProposerImpl.newProposer(
                        ourLearner,
                        ImmutableList.copyOf(acceptors),
                        ImmutableList.copyOf(learners),
                        getQuorumSize(acceptors),
                        UUID.randomUUID(),
                        executor),
                client);

        PaxosSynchronizer.synchronizeLearner(ourLearner, learners);

        return AwaitingLeadershipProxy.newProxyInstance(
                ManagedTimestampService.class,
                () -> createManagedPaxosTimestampService(proposer, client, acceptors, learners),
                leaderElectionService);
    }

    private ManagedTimestampService createManagedPaxosTimestampService(
            PaxosProposer proposer,
            String client,
            List<PaxosAcceptor> acceptors,
            List<PaxosLearner> learners) {
        TimestampBoundStore boundStore = instrument(TimestampBoundStore.class,
                new PaxosTimestampBoundStore(
                        proposer,
                        paxosResource.getPaxosLearner(client),
                        ImmutableList.copyOf(acceptors),
                        ImmutableList.copyOf(learners),
                        paxosConfiguration.maximumWaitBeforeProposalMs()),
                client);
        PersistentTimestampService persistentTimestampService = PersistentTimestampService.create(boundStore);
        return new DelegatingManagedTimestampService(persistentTimestampService, persistentTimestampService);
    }

    private static Set<String> getRemoteServerAddresses(TimeLockServerConfiguration configuration) {
        return Sets.difference(configuration.cluster().servers(),
                ImmutableSet.of(configuration.cluster().localServer()));
    }

    private Set<String> getRemoteServerPaths(TimeLockServerConfiguration configuration) {
        return addProtocols(getRemoteServerAddresses(configuration));
    }

    private Set<String> getAllServerPaths(TimeLockServerConfiguration configuration) {
        return addProtocols(configuration.cluster().servers());
    }

    private String addProtocol(String address) {
        String protocolPrefix = paxosConfiguration.sslConfiguration().isPresent()
                ? "https://" : "http://";
        return protocolPrefix + address;
    }

    private Set<String> addProtocols(Set<String> addresses) {
        return addresses.stream()
                .map(this::addProtocol)
                .collect(Collectors.toSet());
    }
}
