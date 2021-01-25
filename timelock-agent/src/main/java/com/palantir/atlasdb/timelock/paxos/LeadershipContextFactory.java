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

import com.palantir.atlasdb.timelock.paxos.LeadershipComponents.LeadershipContext;
import com.palantir.leader.BatchingLeaderElectionService;
import com.palantir.leader.PaxosLeadershipEventRecorder;
import com.palantir.leader.PingableLeader;
import com.palantir.leader.health.LeaderElectionHealthCheck;
import com.palantir.leader.proxy.LeadershipCoordinator;
import com.palantir.paxos.Client;
import com.palantir.paxos.LeaderPinger;
import com.palantir.paxos.PaxosLearner;
import com.palantir.timelock.paxos.HealthCheckPinger;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.immutables.value.Value;

@Value.Immutable
public abstract class LeadershipContextFactory
        implements NetworkClientFactories.Factory<LeadershipContext>,
                Dependencies.NetworkClientFactories,
                Dependencies.LeaderPinger,
                Dependencies.ClientAwareComponents,
                Dependencies.HealthCheckPinger {

    abstract PaxosResourcesFactory.TimelockPaxosInstallationContext install();

    abstract Factories.LeaderPingHealthCheckFactory healthCheckPingersFactory();

    abstract NetworkClientFactories.Builder networkClientFactoryBuilder();

    abstract Factories.LeaderPingerFactoryContainer.Builder leaderPingerFactoryBuilder();

    @Override
    public abstract Factories.PaxosLatestRoundVerifierFactory latestRoundVerifierFactory();

    @Value.Derived
    @Override
    public int quorumSize() {
        return install().quorumSize();
    }

    @Override
    @Value.Derived
    public LocalPaxosComponents components() {
        return LocalPaxosComponents.createWithBlockingMigration(
                metrics(),
                useCase(),
                install().dataDirectory(),
                install().sqliteDataSource(),
                leaderUuid(),
                install().install().paxos().canCreateNewClients(),
                install().timeLockVersion(),
                install()
                        .install()
                        .iAmOnThePersistenceTeamAndKnowWhatImDoingSkipSqliteConsistencyCheckAndTruncateFileBasedLog());
    }

    @Override
    @Value.Derived
    public UUID leaderUuid() {
        return install().nodeUuid();
    }

    @Override
    @Value.Derived
    public NetworkClientFactories networkClientFactories() {
        return networkClientFactoryBuilder().from(this).build();
    }

    @Override
    @Value.Derived
    public Factories.LeaderPingerFactoryContainer leaderPingerFactory() {
        return leaderPingerFactoryBuilder().from(this).build();
    }

    @Override
    @Value.Derived
    public Duration leaderPingResponseWait() {
        return runtime().get().leaderPingResponseWait();
    }

    @Override
    @Value.Derived
    public Duration leaderPingRate() {
        return runtime().get().pingRate();
    }

    @Value.Derived
    LocalAndRemotes<HealthCheckPinger> healthCheckPingers() {
        return healthCheckPingersFactory().create(this);
    }

    @Value.Derived
    LeaderElectionServiceFactory leaderElectionServiceFactory() {
        return new LeaderElectionServiceFactory();
    }

    @Value.Derived
    public LeaderElectionHealthCheck leaderElectionHealthCheck() {
        return new LeaderElectionHealthCheck(Instant::now);
    }

    @Value.Derived
    LeadershipCoordinatorFactory leadershipCoordinatorFactory() {
        return new LeadershipCoordinatorFactory();
    }

    @Override
    public LeadershipContext create(Client client) {
        ClientAwareComponents clientAwareComponents = ImmutableClientAwareComponents.builder()
                .from(this)
                .proxyClient(client)
                .leaderElectionHealthCheck(leaderElectionHealthCheck())
                .build();

        BatchingLeaderElectionService leaderElectionService =
                leaderElectionServiceFactory().create(clientAwareComponents);

        LeadershipCoordinator leadershipCoordinator =
                leadershipCoordinatorFactory().create(leaderElectionService);

        return ImmutableLeadershipContext.builder()
                .leadershipMetrics(clientAwareComponents.leadershipMetrics())
                .leaderElectionService(leaderElectionService)
                .addCloseables(leaderElectionService)
                .leadershipCoordinator(leadershipCoordinator)
                .addCloseables(leadershipCoordinator)
                .addAllCloseables(leaderPingerFactory().closeables())
                .build();
    }

    @Override
    @Value.Derived
    public AutobatchingLeadershipObserverFactory leadershipObserverFactory() {
        return TimelockLeadershipMetrics.createFactory(metrics());
    }

    @Value.Immutable
    abstract static class ClientAwareComponents
            implements Dependencies.ClientAwareComponents,
                    Dependencies.LeaderElectionService,
                    Dependencies.LeadershipMetrics {

        @Override
        public abstract Client proxyClient();

        @Override
        @Value.Derived
        public Client paxosClient() {
            return useCase().resolveClient(proxyClient());
        }

        @Override
        @Value.Derived
        public PaxosLearner localLearner() {
            return components().learner(paxosClient());
        }

        @Override
        @Value.Derived
        public PingableLeader localPingableLeader() {
            return components().pingableLeader(paxosClient());
        }

        @Override
        @Value.Derived
        public LeaderPinger leaderPinger() {
            return leaderPingerFactory().get().create(paxosClient());
        }

        @Value.Derived
        TimelockLeadershipMetrics leadershipMetrics() {
            return ImmutableTimelockLeadershipMetrics.builder().from(this).build();
        }

        @Override
        @Value.Derived
        public PaxosLeadershipEventRecorder eventRecorder() {
            return leadershipMetrics().eventRecorder();
        }
    }
}
