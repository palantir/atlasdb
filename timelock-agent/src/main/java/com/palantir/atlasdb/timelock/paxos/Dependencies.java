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

import com.palantir.leader.PaxosLeadershipEventRecorder;
import com.palantir.leader.PingableLeader;
import com.palantir.leader.health.LeaderElectionHealthCheck;
import com.palantir.paxos.Client;
import com.palantir.paxos.PaxosLearner;
import com.palantir.timelock.config.PaxosRuntimeConfiguration;
import java.time.Duration;
import java.util.UUID;
import java.util.function.Supplier;
import javax.sql.DataSource;

public interface Dependencies {

    interface LeadershipMetrics {
        AutobatchingLeadershipObserverFactory leadershipObserverFactory();

        TimelockPaxosMetrics metrics();

        PingableLeader localPingableLeader();

        UUID leaderUuid();

        Client proxyClient();

        LeaderElectionHealthCheck leaderElectionHealthCheck();
    }

    interface LeaderPinger {
        PaxosRemoteClients remoteClients();

        UUID leaderUuid();

        Duration leaderPingRate();

        Duration leaderPingResponseWait();

        DataSource dataSource();
    }

    interface NetworkClientFactories {
        PaxosUseCase useCase();

        TimelockPaxosMetrics metrics();

        PaxosRemoteClients remoteClients();

        LocalPaxosComponents components();

        int quorumSize();
    }

    interface ClientAwareComponents {
        LocalPaxosComponents components();

        PaxosUseCase useCase();

        TimelockPaxosMetrics metrics();

        UUID leaderUuid();

        Factories.LeaderPingerFactoryContainer leaderPingerFactory();

        com.palantir.atlasdb.timelock.paxos.NetworkClientFactories networkClientFactories();

        Supplier<PaxosRuntimeConfiguration> runtime();

        AutobatchingLeadershipObserverFactory leadershipObserverFactory();

        Factories.PaxosLatestRoundVerifierFactory latestRoundVerifierFactory();
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

        Factories.PaxosLatestRoundVerifierFactory latestRoundVerifierFactory();
    }

    interface HealthCheckPinger {
        LocalPaxosComponents components();

        PaxosRemoteClients remoteClients();
    }
}
