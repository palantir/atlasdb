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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import com.palantir.atlasdb.config.AuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.config.RemotingClientConfig;
import com.palantir.atlasdb.config.RemotingClientConfigs;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.http.NotCurrentLeaderExceptionMapper;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.streams.KeyedStream;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.config.ssl.TrustContext;
import com.palantir.leader.BatchingLeaderElectionService;
import com.palantir.leader.LeaderElectionService;
import com.palantir.leader.LeaderElectionServiceBuilder;
import com.palantir.leader.LeadershipObserver;
import com.palantir.leader.LocalPingableLeader;
import com.palantir.leader.PaxosLeadershipEventRecorder;
import com.palantir.leader.PingableLeader;
import com.palantir.leader.proxy.LeadershipCoordinator;
import com.palantir.paxos.ImmutableLeaderPingerContext;
import com.palantir.paxos.LeaderPinger;
import com.palantir.paxos.LeaderPingerContext;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosAcceptorImpl;
import com.palantir.paxos.PaxosAcceptorNetworkClient;
import com.palantir.paxos.PaxosConstants;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosLearnerImpl;
import com.palantir.paxos.PaxosLearnerNetworkClient;
import com.palantir.paxos.PaxosProposer;
import com.palantir.paxos.SingleLeaderAcceptorNetworkClient;
import com.palantir.paxos.SingleLeaderLearnerNetworkClient;
import com.palantir.paxos.SingleLeaderPinger;
import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.immutables.value.Value;

public final class Leaders {
    private Leaders() {
        // Utility class
    }

    /**
     * Creates a LeaderElectionService using the supplied configuration and registers appropriate endpoints for that
     * service.
     */
    public static LocalPaxosServices createAndRegisterLocalServices(
            MetricsManager metricsManager, Consumer<Object> env, LeaderConfig config, UserAgent userAgent) {
        LocalPaxosServices localPaxosServices = createInstrumentedLocalServices(metricsManager, config, userAgent);

        env.accept(localPaxosServices.ourAcceptor());
        env.accept(localPaxosServices.ourLearner());
        env.accept(localPaxosServices.localPingableLeader());
        env.accept(new NotCurrentLeaderExceptionMapper());
        return localPaxosServices;
    }

    public static LocalPaxosServices createInstrumentedLocalServices(
            MetricsManager metricsManager, LeaderConfig config, UserAgent userAgent) {
        Set<String> remoteLeaderUris = new HashSet<>(config.leaders());
        remoteLeaderUris.remove(config.localServer());

        RemotePaxosServerSpec remotePaxosServerSpec = ImmutableRemotePaxosServerSpec.builder()
                .remoteLeaderUris(remoteLeaderUris)
                .remoteAcceptorUris(remoteLeaderUris)
                .remoteLearnerUris(remoteLeaderUris)
                .build();
        return createInstrumentedLocalServices(
                metricsManager,
                config,
                remotePaxosServerSpec,
                () -> RemotingClientConfigs.DEFAULT,
                userAgent,
                LeadershipObserver.NO_OP);
    }

    public static LocalPaxosServices createInstrumentedLocalServices(
            MetricsManager metricsManager,
            LeaderConfig config,
            RemotePaxosServerSpec remotePaxosServerSpec,
            Supplier<RemotingClientConfig> remotingClientConfig,
            UserAgent userAgent,
            LeadershipObserver leadershipObserver) {
        UUID leaderUuid = UUID.randomUUID();

        PaxosLeadershipEventRecorder leadershipEventRecorder = PaxosLeadershipEventRecorder.create(
                metricsManager.getTaggedRegistry(), leaderUuid.toString(), leadershipObserver, ImmutableList.of());

        PaxosAcceptor ourAcceptor = AtlasDbMetrics.instrumentTimed(
                metricsManager.getRegistry(),
                PaxosAcceptor.class,
                PaxosAcceptorImpl.newAcceptor(config.acceptorLogDir().getPath()));
        PaxosLearner ourLearner = AtlasDbMetrics.instrumentTimed(
                metricsManager.getRegistry(),
                PaxosLearner.class,
                PaxosLearnerImpl.newLearner(config.learnerLogDir().getPath(), leadershipEventRecorder));

        Optional<TrustContext> trustContext = ServiceCreator.createTrustContext(config.sslConfiguration());

        List<PaxosLearner> learners = createProxyAndLocalList(
                ourLearner,
                remotePaxosServerSpec.remoteLearnerUris(),
                remotingClientConfig,
                trustContext,
                PaxosLearner.class,
                userAgent);
        List<PaxosLearner> remoteLearners = learners.stream()
                .filter(learner -> !learner.equals(ourLearner))
                .collect(ImmutableList.toImmutableList());
        PaxosLearnerNetworkClient learnerNetworkClient = SingleLeaderLearnerNetworkClient.createLegacy(
                ourLearner,
                remoteLearners,
                config.quorumSize(),
                createExecutorsForService(metricsManager, learners, "knowledge-update"),
                PaxosConstants.CANCEL_REMAINING_CALLS);

        List<PaxosAcceptor> acceptors = createProxyAndLocalList(
                ourAcceptor,
                remotePaxosServerSpec.remoteAcceptorUris(),
                remotingClientConfig,
                trustContext,
                PaxosAcceptor.class,
                userAgent);
        PaxosAcceptorNetworkClient acceptorNetworkClient = SingleLeaderAcceptorNetworkClient.createLegacy(
                acceptors,
                config.quorumSize(),
                createExecutorsForService(metricsManager, acceptors, "latest-round-verifier"),
                PaxosConstants.CANCEL_REMAINING_CALLS);

        List<LeaderPingerContext<PingableLeader>> otherLeaders = generatePingables(
                remotePaxosServerSpec.remoteLeaderUris(), remotingClientConfig, trustContext, userAgent);

        LeaderPinger leaderPinger = SingleLeaderPinger.createLegacy(
                createExecutorsForService(metricsManager, otherLeaders, "leader-ping"),
                config.leaderPingResponseWait(),
                leaderUuid,
                PaxosConstants.CANCEL_REMAINING_CALLS);

        LeaderElectionService uninstrumentedLeaderElectionService = new LeaderElectionServiceBuilder()
                .leaderUuid(leaderUuid)
                .knowledge(ourLearner)
                .eventRecorder(leadershipEventRecorder)
                .randomWaitBeforeProposingLeadership(config.randomWaitBeforeProposingLeadership())
                .pingRate(config.pingRate())
                .leaderPinger(leaderPinger)
                .acceptorClient(acceptorNetworkClient)
                .learnerClient(learnerNetworkClient)
                .decorateProposer(proposer ->
                        AtlasDbMetrics.instrumentTimed(metricsManager.getRegistry(), PaxosProposer.class, proposer))
                .leaderAddressCacheTtl(config.leaderAddressCacheTtl())
                .build();

        LeaderElectionService leaderElectionService = AtlasDbMetrics.instrumentTimed(
                metricsManager.getRegistry(), LeaderElectionService.class, uninstrumentedLeaderElectionService);
        PingableLeader pingableLeader = AtlasDbMetrics.instrumentTimed(
                metricsManager.getRegistry(), PingableLeader.class, new LocalPingableLeader(ourLearner, leaderUuid));

        List<PingableLeader> remotePingableLeaders =
                otherLeaders.stream().map(LeaderPingerContext::pinger).collect(Collectors.toList());
        BatchingLeaderElectionService batchingLeaderElectionService =
                new BatchingLeaderElectionService(leaderElectionService);
        return ImmutableLocalPaxosServices.builder()
                .ourAcceptor(ourAcceptor)
                .ourLearner(ourLearner)
                .leaderElectionService(batchingLeaderElectionService)
                .leadershipCoordinator(LeadershipCoordinator.create(batchingLeaderElectionService))
                .localPingableLeader(pingableLeader)
                .remotePingableLeaders(remotePingableLeaders)
                .build();
    }

    private static <T> Map<T, ExecutorService> createExecutorsForService(
            MetricsManager metricsManager, List<T> services, String useCase) {
        Map<T, ExecutorService> executors = new HashMap<>();
        for (int index = 0; index < services.size(); index++) {
            String indexedUseCase = String.format("%s-%d", useCase, index);
            executors.put(services.get(index), createExecutor(metricsManager, indexedUseCase, services.size()));
        }
        return executors;
    }

    // TODO (jkong): Make the limits configurable.
    // Current use cases tend to have not more than 10 (<< 100) inflight tasks under normal circumstances.
    private static ExecutorService createExecutor(MetricsManager metricsManager, String useCase, int corePoolSize) {
        return PTExecutors.newCachedThreadPoolWithMaxThreads(100, "atlas-leaders-election-" + useCase);
    }

    public static <T> List<T> createProxyAndLocalList(
            T localObject,
            Set<String> remoteUris,
            Supplier<RemotingClientConfig> remotingClientConfig,
            Optional<TrustContext> trustContext,
            Class<T> clazz,
            UserAgent userAgent) {

        // TODO (jkong): Enable runtime config for leader election services.
        List<T> remotes = remoteUris.stream()
                .map(uri -> AtlasDbHttpClients.createProxy(
                        trustContext,
                        uri,
                        clazz,
                        AuxiliaryRemotingParameters.builder()
                                .userAgent(userAgent)
                                .shouldLimitPayload(false)
                                .shouldRetry(true)
                                .shouldUseExtendedTimeout(false)
                                .remotingClientConfig(remotingClientConfig)
                                .build()))
                .collect(Collectors.toList());

        return ImmutableList.copyOf(Iterables.concat(remotes, ImmutableList.of(localObject)));
    }

    public static List<LeaderPingerContext<PingableLeader>> generatePingables(
            Collection<String> remoteEndpoints,
            Supplier<RemotingClientConfig> remotingClientConfig,
            Optional<TrustContext> trustContext,
            UserAgent userAgent) {
        return KeyedStream.of(remoteEndpoints)
                .mapKeys(endpoint -> AtlasDbHttpClients.createProxy(
                        trustContext,
                        endpoint,
                        PingableLeader.class,
                        AuxiliaryRemotingParameters.builder()
                                .userAgent(userAgent)
                                .shouldLimitPayload(false) // Guaranteed to be small, no need to limit.
                                .shouldRetry(false)
                                .shouldUseExtendedTimeout(false)
                                .remotingClientConfig(remotingClientConfig)
                                .build()))
                .map(Leaders::convertAddressToHostAndPort)
                .map(ImmutableLeaderPingerContext::of)
                .values()
                .collect(Collectors.toList());
    }

    private static HostAndPort convertAddressToHostAndPort(String url) {
        URI uri = URI.create(url);
        return HostAndPort.fromParts(uri.getHost(), uri.getPort());
    }

    @Value.Immutable
    public interface LocalPaxosServices {
        PaxosAcceptor ourAcceptor();

        PaxosLearner ourLearner();

        LeaderElectionService leaderElectionService();

        LeadershipCoordinator leadershipCoordinator();

        PingableLeader localPingableLeader();

        Set<PingableLeader> remotePingableLeaders();

        @Value.Derived
        default Supplier<Boolean> isCurrentSuspectedLeader() {
            return localPingableLeader()::ping;
        }

        @Value.Derived
        default Set<PingableLeader> allPingableLeaders() {
            return Sets.union(ImmutableSet.of(localPingableLeader()), remotePingableLeaders());
        }
    }

    @Value.Immutable
    public interface RemotePaxosServerSpec {
        Set<String> remoteLeaderUris();

        Set<String> remoteAcceptorUris();

        Set<String> remoteLearnerUris();
    }
}
