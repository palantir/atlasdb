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
package com.palantir.leader;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.net.HostAndPort;
import com.palantir.paxos.LeaderPinger;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosAcceptorNetworkClient;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosLearnerNetworkClient;
import com.palantir.paxos.SingleLeaderAcceptorNetworkClient;
import com.palantir.paxos.SingleLeaderLearnerNetworkClient;
import com.palantir.paxos.SingleLeaderPinger;

@SuppressWarnings({"HiddenField", "OverloadMethodsDeclarationOrder"})
public class PaxosLeaderElectionServiceBuilder {
    private PaxosLearner knowledge;
    private List<PingableLeader> otherPingables;
    private List<PaxosAcceptor> acceptors;
    private List<PaxosLearner> learners;
    private Function<String, ExecutorService> executorServiceFactory;
    private long pingRateMs;
    private long randomWaitBeforeProposingLeadershipMs;
    private Duration leaderPingResponseWait;
    private PaxosLeaderElectionEventRecorder eventRecorder = PaxosLeaderElectionEventRecorder.NO_OP;
    private int quorumSize = -1;
    private UUID leaderUuid;

    public PaxosLeaderElectionServiceBuilder knowledge(PaxosLearner knowledge) {
        this.knowledge = knowledge;
        return this;
    }

    /**
     * Mapping containing the host/port information for all nodes in the cluster EXCEPT this one.
     */
    public PaxosLeaderElectionServiceBuilder potentialLeadersToHosts(
            Map<PingableLeader, HostAndPort> potentialLeadersToHosts) {
        this.otherPingables = ImmutableList.copyOf(potentialLeadersToHosts.keySet());
        return this;
    }

    public PaxosLeaderElectionServiceBuilder acceptors(List<PaxosAcceptor> acceptors) {
        this.acceptors = ImmutableList.copyOf(acceptors);
        return this;
    }

    public PaxosLeaderElectionServiceBuilder learners(List<PaxosLearner> learners) {
        this.learners = ImmutableList.copyOf(learners);
        return this;
    }

    public PaxosLeaderElectionServiceBuilder executor(ExecutorService executor) {
        this.executorServiceFactory = unused -> executor;
        return this;
    }

    /**
     * It is expected that the Strings provided to this function are used for instrumentation purposes only.
     */
    public PaxosLeaderElectionServiceBuilder executorServiceFactory(Function<String, ExecutorService> factory) {
        this.executorServiceFactory = factory;
        return this;
    }

    public PaxosLeaderElectionServiceBuilder pingRateMs(long pingRateMs) {
        this.pingRateMs = pingRateMs;
        return this;
    }

    public PaxosLeaderElectionServiceBuilder randomWaitBeforeProposingLeadershipMs(
            long randomWaitBeforeProposingLeadershipMs) {
        this.randomWaitBeforeProposingLeadershipMs = randomWaitBeforeProposingLeadershipMs;
        return this;
    }

    public PaxosLeaderElectionServiceBuilder leaderPingResponseWaitMs(long leaderPingResponseWaitMs) {
        this.leaderPingResponseWait = Duration.ofMillis(leaderPingResponseWaitMs);
        return this;
    }

    public PaxosLeaderElectionServiceBuilder eventRecorder(PaxosLeaderElectionEventRecorder eventRecorder) {
        this.eventRecorder = eventRecorder;
        return this;
    }

    public PaxosLeaderElectionServiceBuilder quorumSize(int quorumSize) {
        this.quorumSize = quorumSize;
        return this;
    }

    public PaxosLeaderElectionServiceBuilder leaderUuid(UUID leaderUuid) {
        this.leaderUuid = leaderUuid;
        return this;
    }

    private static Map<PaxosLearner, ExecutorService> createKnowledgeUpdateExecutors(
            List<PaxosLearner> paxosLearners,
            Function<String, ExecutorService> executorServiceFactory) {
        return createExecutorsForService(paxosLearners, "knowledge-update", executorServiceFactory);
    }

    private static Map<PaxosAcceptor, ExecutorService> createLatestRoundVerifierExecutors(
            List<PaxosAcceptor> paxosAcceptors,
            Function<String, ExecutorService> executorServiceFactory) {
        return createExecutorsForService(paxosAcceptors, "latest-round-verifier", executorServiceFactory);
    }

    private static Map<PingableLeader, ExecutorService> createLeaderPingExecutors(
            List<PingableLeader> allPingables,
            Function<String, ExecutorService> executorServiceFactory) {
        return createExecutorsForService(allPingables, "leader-ping", executorServiceFactory);
    }

    private static <T> Map<T, ExecutorService> createExecutorsForService(
            List<T> services,
            String useCase,
            Function<String, ExecutorService> executorServiceFactory) {
        Map<T, ExecutorService> executors = Maps.newHashMap();
        for (int index = 0; index < services.size(); index++) {
            executors.put(services.get(index), executorServiceFactory.apply(String.format("%s-%d", useCase, index)));
        }
        return executors;
    }

    private PaxosLearnerNetworkClient buildLearnerNetworkClient() {
        List<PaxosLearner> remoteLearners = learners.stream()
                .filter(learner -> !learner.equals(knowledge))
                .collect(ImmutableList.toImmutableList());
        return new SingleLeaderLearnerNetworkClient(
                knowledge,
                remoteLearners,
                quorumSize,
                createKnowledgeUpdateExecutors(learners, executorServiceFactory));
    }

    private PaxosAcceptorNetworkClient buildAcceptorNetworkClient() {
        return new SingleLeaderAcceptorNetworkClient(
                acceptors,
                quorumSize,
                createLatestRoundVerifierExecutors(acceptors, executorServiceFactory));
    }

    private LeaderPinger buildLeaderPinger(List<PingableLeader> otherPingables) {
        return new SingleLeaderPinger(
                createLeaderPingExecutors(otherPingables, executorServiceFactory),
                leaderPingResponseWait,
                eventRecorder,
                leaderUuid);
    }

    public LeaderElectionService build() {
        return new LeaderElectionServiceBuilder()
                .leaderUuid(leaderUuid)
                .acceptorClient(buildAcceptorNetworkClient())
                .learnerClient(buildLearnerNetworkClient())
                .knowledge(knowledge)
                .leaderPinger(buildLeaderPinger(otherPingables))
                .pingRate(Duration.ofMillis(pingRateMs))
                .randomWaitBeforeProposingLeadership(Duration.ofMillis(randomWaitBeforeProposingLeadershipMs))
                .eventRecorder(eventRecorder)
                .build();
    }
}
