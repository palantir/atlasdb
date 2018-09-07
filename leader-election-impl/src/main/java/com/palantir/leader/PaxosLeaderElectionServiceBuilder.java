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

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosProposer;

public class PaxosLeaderElectionServiceBuilder {
    private PaxosProposer proposer;
    private PaxosLearner knowledge;
    private Map<PingableLeader, HostAndPort> potentialLeadersToHosts;
    private List<PaxosAcceptor> acceptors;
    private List<PaxosLearner> learners;
    private ExecutorService executor;
    private long pingRateMs;
    private long randomWaitBeforeProposingLeadershipMs;
    private long noQuorumMaxDelayMs = PaxosLeaderElectionService.DEFAULT_NO_QUORUM_MAX_DELAY_MS;
    private long leaderPingResponseWaitMs;
    private PaxosLeaderElectionEventRecorder eventRecorder = PaxosLeaderElectionEventRecorder.NO_OP;

    public PaxosLeaderElectionServiceBuilder proposer(PaxosProposer proposer) {
        this.proposer = proposer;
        return this;
    }

    public PaxosLeaderElectionServiceBuilder knowledge(PaxosLearner knowledge) {
        this.knowledge = knowledge;
        return this;
    }

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
        this.executor = executor;
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

    public PaxosLeaderElectionServiceBuilder noQuorumMaxDelayMs(long noQuorumMaxDelayMs) {
        this.noQuorumMaxDelayMs = noQuorumMaxDelayMs;
        return this;
    }

    public PaxosLeaderElectionServiceBuilder eventRecorder(PaxosLeaderElectionEventRecorder eventRecorder) {
        this.eventRecorder = eventRecorder;
        return this;
    }

    public PaxosLeaderElectionService build() {
        return new PaxosLeaderElectionService(
                proposer,
                knowledge,
                potentialLeadersToHosts,
                acceptors,
                learners,
                executor,
                pingRateMs,
                randomWaitBeforeProposingLeadershipMs,
                noQuorumMaxDelayMs,
                leaderPingResponseWaitMs,
                eventRecorder);
    }
}
