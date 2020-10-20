/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.paxos;

import com.palantir.leader.BatchingLeaderElectionService;
import com.palantir.leader.LeaderElectionServiceBuilder;
import com.palantir.paxos.Client;
import com.palantir.paxos.PaxosAcceptorNetworkClient;
import com.palantir.paxos.PaxosProposer;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LeaderElectionServiceFactory {

    private final Map<Client, BatchingLeaderElectionService> leaderElectionServicesByClient = new ConcurrentHashMap<>();

    public BatchingLeaderElectionService create(Dependencies.LeaderElectionService dependencies) {
        return leaderElectionServicesByClient.computeIfAbsent(
                dependencies.paxosClient(), _client -> createNewInstance(dependencies));
    }

    private static BatchingLeaderElectionService createNewInstance(Dependencies.LeaderElectionService dependencies) {
        PaxosAcceptorNetworkClient acceptorClient =
                dependencies.networkClientFactories().acceptor().create(dependencies.paxosClient());

        return new BatchingLeaderElectionService(new LeaderElectionServiceBuilder()
                .leaderPinger(dependencies.leaderPinger())
                .leaderUuid(dependencies.leaderUuid())
                .pingRate(dependencies.runtime().get().pingRate())
                .randomWaitBeforeProposingLeadership(
                        dependencies.runtime().get().maximumWaitBeforeProposingLeadership())
                .eventRecorder(dependencies.eventRecorder())
                .knowledge(dependencies.localLearner())
                .acceptorClient(acceptorClient)
                .learnerClient(dependencies.networkClientFactories().learner().create(dependencies.paxosClient()))
                .latestRoundVerifier(dependencies.latestRoundVerifierFactory().create(acceptorClient))
                .decorateProposer(uninstrumentedPaxosProposer -> instrumentProposer(
                        dependencies.paxosClient(), dependencies.metrics(), uninstrumentedPaxosProposer))
                .leaderAddressCacheTtl(Duration.ofSeconds(1))
                .build());
    }

    private static PaxosProposer instrumentProposer(
            Client paxosClient, TimelockPaxosMetrics metrics, PaxosProposer uninstrumentedPaxosProposer) {
        return metrics.instrument(PaxosProposer.class, uninstrumentedPaxosProposer, paxosClient);
    }
}
