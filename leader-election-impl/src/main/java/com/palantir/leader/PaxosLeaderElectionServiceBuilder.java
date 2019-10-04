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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.net.HostAndPort;
import com.palantir.paxos.LeaderPinger;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosLatestRoundVerifier;
import com.palantir.paxos.PaxosLatestRoundVerifierImpl;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosLearnerNetworkClient;
import com.palantir.paxos.PaxosProposer;
import com.palantir.paxos.SingleLeaderAcceptorNetworkClient;
import com.palantir.paxos.SingleLeaderLearnerNetworkClient;
import com.palantir.paxos.SingleLeaderPinger;

@SuppressWarnings("HiddenField")
public class PaxosLeaderElectionServiceBuilder {
    private PaxosProposer proposer;
    private PaxosLearner knowledge;
    private List<PingableLeader> otherPingables;
    private List<PaxosAcceptor> acceptors;
    private List<PaxosLearner> learners;
    private Function<String, ExecutorService> executorServiceFactory;
    private long pingRateMs;
    private long randomWaitBeforeProposingLeadershipMs;
    private Duration leaderPingResponseWait;
    private PaxosLeaderElectionEventRecorder eventRecorder = PaxosLeaderElectionEventRecorder.NO_OP;

    public PaxosLeaderElectionServiceBuilder proposer(PaxosProposer proposer) {
        this.proposer = proposer;
        return this;
    }

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

    private static Map<PaxosLearner, ExecutorService> createKnowledgeUpdateExecutors(
            List<PaxosLearner> paxosLearners,
            Function<String, ExecutorService> executorServiceFactory) {
        return IntStream.range(0, paxosLearners.size())
                .boxed()
                .collect(Collectors.toMap(
                        paxosLearners::get,
                        index -> executorServiceFactory.apply("knowledge-update-" + index)));
    }

    private static Map<PaxosAcceptor, ExecutorService> createLatestRoundVerifierExecutors(
            List<PaxosAcceptor> paxosAcceptors,
            Function<String, ExecutorService> executorServiceFactory) {
        Map<PaxosAcceptor, ExecutorService> executors = Maps.newHashMap();
        for (int i = 0; i < paxosAcceptors.size(); i++) {
            executors.put(paxosAcceptors.get(i), executorServiceFactory.apply("latest-round-verifier-" + i));
        }
        return executors;
    }

    private static Map<PingableLeader, ExecutorService> createLeaderPingExecutors(
            List<PingableLeader> allPingables,
            Function<String, ExecutorService> executorServiceFactory) {
        Map<PingableLeader, ExecutorService> executors = Maps.newHashMap();
        for (int i = 0; i < allPingables.size(); i++) {
            executors.put(allPingables.get(i), executorServiceFactory.apply("leader-ping-" + i));
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
                proposer.getQuorumSize(),
                createKnowledgeUpdateExecutors(learners, executorServiceFactory));
    }

    private PaxosLatestRoundVerifier buildLatestRoundVerifier() {
        SingleLeaderAcceptorNetworkClient acceptorClient = new SingleLeaderAcceptorNetworkClient(
                acceptors,
                proposer.getQuorumSize(),
                createLatestRoundVerifierExecutors(acceptors, executorServiceFactory));
        return new PaxosLatestRoundVerifierImpl(acceptorClient);
    }

    private LeaderPinger buildLeaderPinger(List<PingableLeader> otherPingables) {
        return new SingleLeaderPinger(
                createLeaderPingExecutors(otherPingables, executorServiceFactory),
                leaderPingResponseWait,
                eventRecorder,
                UUID.fromString(proposer.getUuid()));
    }

    public PaxosLeaderElectionService build() {
        return new PaxosLeaderElectionService(
                proposer,
                knowledge,
                buildLeaderPinger(otherPingables),
                new LocalPingableLeader(knowledge, UUID.fromString(proposer.getUuid())),
                otherPingables,
                acceptors,
                buildLatestRoundVerifier(),
                buildLearnerNetworkClient(),
                pingRateMs,
                randomWaitBeforeProposingLeadershipMs,
                eventRecorder);
    }
}
