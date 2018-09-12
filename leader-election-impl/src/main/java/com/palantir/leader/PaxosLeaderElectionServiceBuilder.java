/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.leader;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosProposer;

@SuppressWarnings("HiddenField")
public class PaxosLeaderElectionServiceBuilder {
    private PaxosProposer proposer;
    private PaxosLearner knowledge;
    private Map<PingableLeader, HostAndPort> potentialLeadersToHosts;
    private List<PaxosAcceptor> acceptors;
    private List<PaxosLearner> learners;
    private Function<String, ExecutorService> executorServiceFactory;
    private long pingRateMs;
    private long randomWaitBeforeProposingLeadershipMs;
    private long leaderPingResponseWaitMs;
    private PaxosLeaderElectionEventRecorder eventRecorder = PaxosLeaderElectionEventRecorder.NO_OP;
    private Supplier<Boolean> onlyLogOnQuorumFailure = () -> true;

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
        this.potentialLeadersToHosts = potentialLeadersToHosts;
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
        this.leaderPingResponseWaitMs = leaderPingResponseWaitMs;
        return this;
    }

    public PaxosLeaderElectionServiceBuilder eventRecorder(PaxosLeaderElectionEventRecorder eventRecorder) {
        this.eventRecorder = eventRecorder;
        return this;
    }

    public PaxosLeaderElectionServiceBuilder onlyLogOnQuorumFailure(Supplier<Boolean> onlyLogOnQuorumFailure) {
        this.onlyLogOnQuorumFailure = onlyLogOnQuorumFailure;
        return this;
    }

    public PaxosLeaderElectionService build() {
        return new PaxosLeaderElectionService(
                proposer,
                knowledge,
                potentialLeadersToHosts,
                acceptors,
                learners,
                executorServiceFactory::apply,
                pingRateMs,
                randomWaitBeforeProposingLeadershipMs,
                leaderPingResponseWaitMs,
                eventRecorder,
                onlyLogOnQuorumFailure);
    }
}
