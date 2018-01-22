/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.factory;

import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

import javax.net.ssl.SSLSocketFactory;

import org.immutables.value.Value;

import com.codahale.metrics.InstrumentedExecutorService;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.http.NotCurrentLeaderExceptionMapper;
import com.palantir.atlasdb.http.UserAgents;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.leader.LeaderElectionService;
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
     * Creates a LeaderElectionService using the supplied configuration and
     * registers appropriate endpoints for that service.
     */
    public static LeaderElectionService create(Consumer<Object> env, LeaderConfig config) {
        return create(env, config, UserAgents.DEFAULT_USER_AGENT);
    }

    public static LeaderElectionService create(Consumer<Object> env, LeaderConfig config, String userAgent) {
        return createAndRegisterLocalServices(env, config, userAgent).leaderElectionService();
    }

    public static LocalPaxosServices createAndRegisterLocalServices(
            Consumer<Object> env, LeaderConfig config, String userAgent) {
        LocalPaxosServices localPaxosServices = createInstrumentedLocalServices(config, userAgent);

        env.accept(localPaxosServices.ourAcceptor());
        env.accept(localPaxosServices.ourLearner());
        env.accept(localPaxosServices.pingableLeader());
        env.accept(new NotCurrentLeaderExceptionMapper());
        return localPaxosServices;
    }

    public static LocalPaxosServices createInstrumentedLocalServices(LeaderConfig config, String userAgent) {
        Set<String> remoteLeaderUris = Sets.newHashSet(config.leaders());
        remoteLeaderUris.remove(config.localServer());

        RemotePaxosServerSpec remotePaxosServerSpec = ImmutableRemotePaxosServerSpec.builder()
                .remoteLeaderUris(remoteLeaderUris)
                .remoteAcceptorUris(remoteLeaderUris)
                .remoteLearnerUris(remoteLeaderUris)
                .build();
        return createInstrumentedLocalServices(config, remotePaxosServerSpec, userAgent);
    }

    public static LocalPaxosServices createInstrumentedLocalServices(
            LeaderConfig config,
            RemotePaxosServerSpec remotePaxosServerSpec,
            String userAgent) {
        UUID leaderUuid = UUID.randomUUID();

        PaxosLeadershipEventRecorder leadershipEventRecorder = PaxosLeadershipEventRecorder.create(
                AtlasDbMetrics.getMetricRegistry(), leaderUuid.toString());

        PaxosAcceptor ourAcceptor = AtlasDbMetrics.instrument(
                PaxosAcceptor.class,
                PaxosAcceptorImpl.newAcceptor(config.acceptorLogDir().getPath()));
        PaxosLearner ourLearner = AtlasDbMetrics.instrument(
                PaxosLearner.class,
                PaxosLearnerImpl.newLearner(config.learnerLogDir().getPath(), leadershipEventRecorder));

        Optional<SSLSocketFactory> sslSocketFactory =
                ServiceCreator.createSslSocketFactory(config.sslConfiguration());

        List<PaxosLearner> learners = createProxyAndLocalList(
                ourLearner, remotePaxosServerSpec.remoteLearnerUris(), sslSocketFactory, PaxosLearner.class, userAgent);
        List<PaxosAcceptor> acceptors = createProxyAndLocalList(
                ourAcceptor,
                remotePaxosServerSpec.remoteAcceptorUris(),
                sslSocketFactory,
                PaxosAcceptor.class,
                userAgent);

        Map<PingableLeader, HostAndPort> otherLeaders = generatePingables(
                remotePaxosServerSpec.remoteLeaderUris(), sslSocketFactory, userAgent);

        InstrumentedExecutorService proposerExecutorService = new InstrumentedExecutorService(
                PTExecutors.newCachedThreadPool(new ThreadFactoryBuilder()
                        .setNameFormat("atlas-proposer-%d")
                        .setDaemon(true)
                        .build()),
                AtlasDbMetrics.getMetricRegistry(),
                MetricRegistry.name(PaxosProposer.class, "executor"));
        PaxosProposer proposer = AtlasDbMetrics.instrument(PaxosProposer.class,
                PaxosProposerImpl.newProposer(ourLearner, acceptors, learners, config.quorumSize(),
                leaderUuid, proposerExecutorService));

        InstrumentedExecutorService leaderElectionExecutor = new InstrumentedExecutorService(
                PTExecutors.newCachedThreadPool(new ThreadFactoryBuilder()
                        .setNameFormat("atlas-leaders-election-%d")
                        .setDaemon(true)
                        .build()),
                AtlasDbMetrics.getMetricRegistry(),
                MetricRegistry.name(PaxosLeaderElectionService.class, "executor"));

        PaxosLeaderElectionService paxosLeaderElectionService = new PaxosLeaderElectionServiceBuilder()
                .proposer(proposer)
                .knowledge(ourLearner)
                .potentialLeadersToHosts(otherLeaders)
                .acceptors(acceptors)
                .learners(learners)
                .executor(leaderElectionExecutor)
                .pingRateMs(config.pingRateMs())
                .randomWaitBeforeProposingLeadershipMs(config.randomWaitBeforeProposingLeadershipMs())
                .leaderPingResponseWaitMs(config.leaderPingResponseWaitMs())
                .eventRecorder(leadershipEventRecorder)
                .build();

        LeaderElectionService leaderElectionService = AtlasDbMetrics.instrument(
                LeaderElectionService.class,
                paxosLeaderElectionService);
        PingableLeader pingableLeader = AtlasDbMetrics.instrument(
                PingableLeader.class,
                paxosLeaderElectionService);

        return ImmutableLocalPaxosServices.builder()
                .ourAcceptor(ourAcceptor)
                .ourLearner(ourLearner)
                .leaderElectionService(leaderElectionService)
                .pingableLeader(pingableLeader)
                .build();
    }

    public static <T> List<T> createProxyAndLocalList(
            T localObject,
            Set<String> remoteUris,
            Optional<SSLSocketFactory> sslSocketFactory,
            Class<T> clazz) {
        return createProxyAndLocalList(localObject, remoteUris, sslSocketFactory, clazz, UserAgents.DEFAULT_USER_AGENT);
    }

    public static <T> List<T> createProxyAndLocalList(
            T localObject,
            Set<String> remoteUris,
            Optional<SSLSocketFactory> sslSocketFactory,
            Class<T> clazz,
            String userAgent) {
        return ImmutableList.copyOf(Iterables.concat(
                AtlasDbHttpClients.createProxies(sslSocketFactory, remoteUris, true, clazz, userAgent),
                ImmutableList.of(localObject)));
    }

    public static Map<PingableLeader, HostAndPort> generatePingables(
            Collection<String> remoteEndpoints,
            Optional<SSLSocketFactory> sslSocketFactory,
            String userAgent) {
        /* The interface used as a key here may be a proxy, which may have strange .equals() behavior.
         * This is circumvented by using an IdentityHashMap which will just use native == for equality.
         */
        Map<PingableLeader, HostAndPort> pingables = new IdentityHashMap<>();
        for (String endpoint : remoteEndpoints) {
            PingableLeader remoteInterface = AtlasDbHttpClients
                    .createProxy(sslSocketFactory, endpoint, true, PingableLeader.class, userAgent);
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
    }

    @Value.Immutable
    public interface RemotePaxosServerSpec {
        Set<String> remoteLeaderUris();
        Set<String> remoteAcceptorUris();
        Set<String> remoteLearnerUris();
    }
}
