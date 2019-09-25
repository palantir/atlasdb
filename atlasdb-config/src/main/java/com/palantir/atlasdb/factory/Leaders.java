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

import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.immutables.value.Value;

import com.codahale.metrics.InstrumentedExecutorService;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.palantir.atlasdb.config.AuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.config.LeaderRuntimeConfig;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.http.AtlasDbRemotingConstants;
import com.palantir.atlasdb.http.NotCurrentLeaderExceptionMapper;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.config.ssl.TrustContext;
import com.palantir.leader.AsyncLeadershipObserver;
import com.palantir.leader.BatchingLeaderElectionService;
import com.palantir.leader.LeaderElectionService;
import com.palantir.leader.LeadershipObserver;
import com.palantir.leader.PaxosLeaderElectionService;
import com.palantir.leader.PaxosLeaderElectionServiceBuilder;
import com.palantir.leader.PaxosLeadershipEventRecorder;
import com.palantir.leader.PingableLeader;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosAcceptorImpl;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosLearnerImpl;
import com.palantir.paxos.PaxosProposer;
import com.palantir.paxos.PaxosProposerImpl;

public final class Leaders {
    private Leaders() {
        // Utility class
    }

    /**
     * Creates a LeaderElectionService using the supplied configuration and registers appropriate endpoints for that
     * service.
     */
    public static LeaderElectionService create(MetricsManager metricsManager,
            Consumer<Object> env, LeaderConfig config, Supplier<LeaderRuntimeConfig> runtime) {
        return create(metricsManager, env, config, runtime, AtlasDbRemotingConstants.DEFAULT_USER_AGENT);
    }

    public static LeaderElectionService create(MetricsManager metricsManager,
            Consumer<Object> env, LeaderConfig config, Supplier<LeaderRuntimeConfig> runtime, UserAgent userAgent) {
        return createAndRegisterLocalServices(metricsManager, env, config, runtime, userAgent).leaderElectionService();
    }

    public static LocalPaxosServices createAndRegisterLocalServices(
            MetricsManager metricsManager,
            Consumer<Object> env,
            LeaderConfig config,
            Supplier<LeaderRuntimeConfig> runtime,
            UserAgent userAgent) {
        LocalPaxosServices localPaxosServices = createInstrumentedLocalServices(
                metricsManager, config, runtime, userAgent);

        env.accept(localPaxosServices.ourAcceptor());
        env.accept(localPaxosServices.ourLearner());
        env.accept(localPaxosServices.pingableLeader());
        env.accept(new NotCurrentLeaderExceptionMapper());
        return localPaxosServices;
    }

    public static LocalPaxosServices createInstrumentedLocalServices(
            MetricsManager metricsManager,
            LeaderConfig config,
            Supplier<LeaderRuntimeConfig> runtime,
            UserAgent userAgent) {
        Set<String> remoteLeaderUris = Sets.newHashSet(config.leaders());
        remoteLeaderUris.remove(config.localServer());

        RemotePaxosServerSpec remotePaxosServerSpec = ImmutableRemotePaxosServerSpec.builder()
                .remoteLeaderUris(remoteLeaderUris)
                .remoteAcceptorUris(remoteLeaderUris)
                .remoteLearnerUris(remoteLeaderUris)
                .build();
        return createInstrumentedLocalServices(metricsManager, config, runtime, remotePaxosServerSpec, userAgent);
    }

    public static LocalPaxosServices createInstrumentedLocalServices(
            MetricsManager metricsManager,
            LeaderConfig config,
            Supplier<LeaderRuntimeConfig> runtime,
            RemotePaxosServerSpec remotePaxosServerSpec,
            UserAgent userAgent) {
        UUID leaderUuid = UUID.randomUUID();

        AsyncLeadershipObserver leadershipObserver = AsyncLeadershipObserver.create();
        PaxosLeadershipEventRecorder leadershipEventRecorder = PaxosLeadershipEventRecorder.create(
                metricsManager.getRegistry(), leaderUuid.toString(), leadershipObserver);

        PaxosAcceptor ourAcceptor = AtlasDbMetrics.instrument(metricsManager.getRegistry(),
                PaxosAcceptor.class,
                PaxosAcceptorImpl.newAcceptor(config.acceptorLogDir().getPath()));
        PaxosLearner ourLearner = AtlasDbMetrics.instrument(metricsManager.getRegistry(),
                PaxosLearner.class,
                PaxosLearnerImpl.newLearner(config.learnerLogDir().getPath(), leadershipEventRecorder));

        Optional<TrustContext> trustContext =
                ServiceCreator.createTrustContext(config.sslConfiguration());

        List<PaxosLearner> learners = createProxyAndLocalList(
                metricsManager.getRegistry(), ourLearner, remotePaxosServerSpec.remoteLearnerUris(),
                trustContext, PaxosLearner.class, userAgent);
        List<PaxosAcceptor> acceptors = createProxyAndLocalList(
                metricsManager.getRegistry(),
                ourAcceptor,
                remotePaxosServerSpec.remoteAcceptorUris(),
                trustContext,
                PaxosAcceptor.class,
                userAgent);

        Map<PingableLeader, HostAndPort> otherLeaders = generatePingables(
                metricsManager, remotePaxosServerSpec.remoteLeaderUris(), trustContext, userAgent);

        InstrumentedExecutorService proposerExecutorService = new InstrumentedExecutorService(
                PTExecutors.newCachedThreadPool(daemonThreadFactory("atlas-proposer")),
                metricsManager.getRegistry(),
                MetricRegistry.name(PaxosProposer.class, "executor"));
        PaxosProposer proposer = AtlasDbMetrics.instrument(metricsManager.getRegistry(), PaxosProposer.class,
                PaxosProposerImpl.newProposer(ourLearner, acceptors, learners, config.quorumSize(),
                        leaderUuid, proposerExecutorService));

        // TODO (jkong): Make the limits configurable.
        // Current use cases tend to have not more than 10 (<< 100) inflight tasks under normal circumstances.
        Function<String, ExecutorService> leaderElectionExecutor = (useCase) -> new InstrumentedExecutorService(
                PTExecutors.newThreadPoolExecutor(
                        otherLeaders.size(),
                        Math.max(otherLeaders.size(), 100),
                        5000,
                        TimeUnit.MILLISECONDS,
                        new SynchronousQueue<>(),
                        daemonThreadFactory("atlas-leaders-election-" + useCase)),
                metricsManager.getRegistry(),
                MetricRegistry.name(PaxosLeaderElectionService.class, useCase, "executor"));

        PaxosLeaderElectionService paxosLeaderElectionService = new PaxosLeaderElectionServiceBuilder()
                .proposer(proposer)
                .knowledge(ourLearner)
                .potentialLeadersToHosts(otherLeaders)
                .acceptors(acceptors)
                .learners(learners)
                .executorServiceFactory(leaderElectionExecutor)
                .pingRateMs(config.pingRateMs())
                .randomWaitBeforeProposingLeadershipMs(config.randomWaitBeforeProposingLeadershipMs())
                .leaderPingResponseWaitMs(config.leaderPingResponseWaitMs())
                .eventRecorder(leadershipEventRecorder)
                .build();

        LeaderElectionService leaderElectionService = AtlasDbMetrics.instrument(metricsManager.getRegistry(),
                LeaderElectionService.class,
                paxosLeaderElectionService);
        PingableLeader pingableLeader = AtlasDbMetrics.instrument(metricsManager.getRegistry(),
                PingableLeader.class,
                paxosLeaderElectionService);

        return ImmutableLocalPaxosServices.builder()
                .ourAcceptor(ourAcceptor)
                .ourLearner(ourLearner)
                .leaderElectionService(new BatchingLeaderElectionService(leaderElectionService))
                .pingableLeader(pingableLeader)
                .leadershipObserver(leadershipObserver)
                .isCurrentSuspectedLeader(paxosLeaderElectionService::ping)
                .build();
    }

    private static ThreadFactory daemonThreadFactory(String name) {
        return new ThreadFactoryBuilder()
                .setNameFormat(name + "-%d")
                .setDaemon(true)
                .build();
    }

    public static <T> List<T> createProxyAndLocalList(
            MetricRegistry metrics,
            T localObject,
            Set<String> remoteUris,
            Optional<TrustContext> trustContext,
            Class<T> clazz,
            UserAgent userAgent) {

        // TODO (jkong): Enable runtime config for leader election services.
        List<T> remotes = remoteUris.stream()
                .map(uri -> AtlasDbHttpClients.createProxy(metrics, trustContext, uri, clazz,
                        AuxiliaryRemotingParameters.builder().userAgent(userAgent).shouldLimitPayload(false).build()))
                .collect(Collectors.toList());

        return ImmutableList.copyOf(Iterables.concat(
                remotes,
                ImmutableList.of(localObject)));
    }

    public static Map<PingableLeader, HostAndPort> generatePingables(
            MetricsManager metricsManager,
            Collection<String> remoteEndpoints,
            Optional<TrustContext> trustContext,
            UserAgent userAgent) {
        /* The interface used as a key here may be a proxy, which may have strange .equals() behavior.
         * This is circumvented by using an IdentityHashMap which will just use native == for equality.
         */
        Map<PingableLeader, HostAndPort> pingables = new IdentityHashMap<>();
        for (String endpoint : remoteEndpoints) {
            PingableLeader remoteInterface = AtlasDbHttpClients.createProxyWithoutRetrying(
                    metricsManager.getRegistry(),
                    trustContext,
                    endpoint,
                    PingableLeader.class,
                    AuxiliaryRemotingParameters.builder() // TODO (jkong): Configurable remoting client config.
                            .shouldLimitPayload(false)
                            .userAgent(userAgent)
                            .build());
            HostAndPort hostAndPort = HostAndPort.fromString(endpoint);
            pingables.put(remoteInterface, hostAndPort);
        }
        return pingables;
    }

    @Value.Immutable
    public interface LocalPaxosServices {
        PaxosAcceptor ourAcceptor();
        PaxosLearner ourLearner();
        LeaderElectionService leaderElectionService();
        PingableLeader pingableLeader();
        LeadershipObserver leadershipObserver();
        Supplier<Boolean> isCurrentSuspectedLeader();
    }

    @Value.Immutable
    public interface RemotePaxosServerSpec {
        Set<String> remoteLeaderUris();
        Set<String> remoteAcceptorUris();
        Set<String> remoteLearnerUris();
    }
}
