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
import java.util.List;
import java.util.UUID;

import org.immutables.value.Value;

import com.palantir.atlasdb.timelock.paxos.LeadershipComponents.LeadershipContext;
import com.palantir.atlasdb.timelock.paxos.NetworkClientFactories.Factory;
import com.palantir.leader.BatchingLeaderElectionService;
import com.palantir.leader.PaxosLeadershipEventRecorder;
import com.palantir.leader.PingableLeader;
import com.palantir.paxos.LeaderPinger;
import com.palantir.paxos.PaxosLearner;

@Value.Immutable
public abstract class LeadershipContextFactory implements
        Factory<LeadershipContext>,
        Dependencies.NetworkClientFactories,
        Dependencies.LeaderPinger,
        Dependencies.ClientAwareComponents,
        Dependencies.HealthCheckPinger {

    abstract PaxosResourcesFactory.TimelockPaxosInstallationContext install();
    abstract Factories.LeaderPingerFactory leaderPingerFactory();
    abstract Factories.LeaderPingHealthCheckFactory healthCheckPingersFactory();
    abstract NetworkClientFactories.Builder networkClientFactoryBuilder();

    @Value.Derived
    @Override
    public int quorumSize() {
        return install().quorumSize();
    }

    @Value.Derived
    public LocalPaxosComponents components() {
        return new LocalPaxosComponents(
                metrics(),
                useCase().logDirectoryRelativeToDataDirectory(install().dataDirectory()),
                leaderUuid());
    }

    @Value.Derived
    public UUID leaderUuid() {
        return install().nodeUuid();
    }

    @Value.Derived
    public NetworkClientFactories networkClientFactories() {
        return networkClientFactoryBuilder().from(this).build();
    }

    @Value.Derived
    public Duration leaderPingResponseWait() {
        return runtime().get().leaderPingResponseWait();
    }

    @Value.Derived
    public LeaderPinger leaderPinger() {
        return leaderPingerFactory().create(this);
    }

    @Value.Derived
    List<com.palantir.timelock.paxos.HealthCheckPinger> healthCheckPingers() {
        return healthCheckPingersFactory().create(this);
    }

    @Value.Derived
    LeaderElectionServiceFactory leaderElectionServiceFactory() {
        return new LeaderElectionServiceFactory();
    }

    @Override
    public LeadershipContext create(Client client) {
        ClientAwareComponents clientAwareComponents = ImmutableClientAwareComponents.builder()
                .from(this)
                .proxyClient(client)
                .build();

        BatchingLeaderElectionService leaderElectionService =
                leaderElectionServiceFactory().create(clientAwareComponents);
        return ImmutableLeadershipContext.builder()
                .leadershipMetrics(clientAwareComponents.leadershipMetrics())
                .leaderElectionService(leaderElectionService)
                .addCloseables(leaderElectionService)
                .build();
    }

    @Value.Derived
    public AutobatchingLeadershipObserverFactory leadershipObserverFactory() {
        return TimelockLeadershipMetrics.createFactory(metrics());
    }

    @Value.Immutable
    abstract static class ClientAwareComponents implements
            Dependencies.ClientAwareComponents,
            Dependencies.LeaderElectionService,
            Dependencies.LeadershipMetrics {

        public abstract Client proxyClient();

        @Value.Derived
        public Client paxosClient() {
            return useCase().resolveClient(proxyClient());
        }

        @Value.Derived
        public PaxosLearner localLearner() {
            return components().learner(paxosClient());
        }

        @Value.Derived
        public PingableLeader localPingableLeader() {
            return components().pingableLeader(paxosClient());
        }

        @Value.Derived
        TimelockLeadershipMetrics leadershipMetrics() {
            return ImmutableTimelockLeadershipMetrics.builder().from(this).build();
        }

        @Value.Derived
        public PaxosLeadershipEventRecorder eventRecorder() {
            return leadershipMetrics().eventRecorder();
        }

    }
}
