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

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import com.palantir.leader.PaxosLeadershipEventRecorder;
import com.palantir.leader.PingableLeader;
import com.palantir.paxos.PaxosLearner;
import com.palantir.timelock.config.PaxosRuntimeConfiguration;

public interface Dependencies {

    interface LeadershipMetrics {
        AutobatchingLeadershipObserverFactory leadershipObserverFactory();
        TimelockPaxosMetrics metrics();
        PingableLeader localPingableLeader();
        UUID leaderUuid();
        Client proxyClient();
    }

    interface LeaderPinger {
        PaxosRemoteClients remoteClients();
        ExecutorService sharedExecutor();
        UUID leaderUuid();
        Duration leaderPingResponseWait();
    }

    interface NetworkClientFactories {
        PaxosUseCase useCase();
        TimelockPaxosMetrics metrics();
        PaxosRemoteClients remoteClients();
        LocalPaxosComponents components();
        int quorumSize();
        ExecutorService sharedExecutor();
    }

    interface ClientAwareComponents {
        LocalPaxosComponents components();
        PaxosUseCase useCase();
        TimelockPaxosMetrics metrics();
        UUID leaderUuid();
        com.palantir.paxos.LeaderPinger leaderPinger();
        com.palantir.atlasdb.timelock.paxos.NetworkClientFactories networkClientFactories();
        Supplier<PaxosRuntimeConfiguration> runtime();
        AutobatchingLeadershipObserverFactory leadershipObserverFactory();
    }

    interface LeaderElectionService {
        Client paxosClient();
        UUID leaderUuid();
        TimelockPaxosMetrics metrics();
        com.palantir.paxos.LeaderPinger leaderPinger();
        Supplier<PaxosRuntimeConfiguration> runtime();
        PaxosLeadershipEventRecorder eventRecorder();
        PaxosLearner localLearner();
        com.palantir.atlasdb.timelock.paxos.NetworkClientFactories networkClientFactories();
    }

    interface LeaderPingHealthCheck {
        LocalPaxosComponents components();
        PaxosRemoteClients remoteClients();
    }

}
