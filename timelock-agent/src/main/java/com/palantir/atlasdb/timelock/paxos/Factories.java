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

import java.io.Closeable;
import java.util.List;

import org.immutables.value.Value;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.timelock.paxos.NetworkClientFactories.Factory;
import com.palantir.paxos.LeaderPinger;
import com.palantir.paxos.SingleLeaderPinger;
import com.palantir.timelock.paxos.HealthCheckPinger;

public interface Factories {
    interface LeaderPingerFactoryContainer {
        Factory<LeaderPinger> get();
        List<Closeable> closeables();

        interface Builder {
            Builder from(Dependencies.LeaderPinger dependencies);
            LeaderPingerFactoryContainer build();
        }
    }

    interface LeaderPingHealthCheckFactory {
        List<HealthCheckPinger> create(Dependencies.HealthCheckPinger dependencies);
    }

    @Value.Immutable
    abstract class BatchingLeaderPingerFactory implements LeaderPingerFactoryContainer, Dependencies.LeaderPinger {

        @Value.Derived
        AutobatchingPingableLeaderFactory pingableLeaderFactory() {
            return AutobatchingPingableLeaderFactory.create(
                    Maps.toMap(remoteClients().batchPingableLeadersWithContext(), _pingableLeader -> sharedExecutor()),
                    leaderPingResponseWait(),
                    leaderUuid(),
                    cancelRemainingCalls());
        }

        @Override
        public Factory<LeaderPinger> get() {
            return pingableLeaderFactory()::leaderPingerFor;
        }

        @Override
        @Value.Derived
        public List<Closeable> closeables() {
            return ImmutableList.of(pingableLeaderFactory());
        }

        public abstract static class Builder implements LeaderPingerFactoryContainer.Builder {}
    }

    @Value.Immutable
    abstract class SingleLeaderPingerFactory implements LeaderPingerFactoryContainer, Dependencies.LeaderPinger {

        @Value.Derived
        SingleLeaderPinger pinger() {
            return new SingleLeaderPinger(
                    Maps.toMap(remoteClients().nonBatchPingableLeadersWithContext(), _pingable -> sharedExecutor()),
                    leaderPingResponseWait(),
                    leaderUuid(),
                    cancelRemainingCalls());
        }

        @Override
        public Factory<LeaderPinger> get() {
            return _client -> pinger();
        }

        public abstract static class Builder implements LeaderPingerFactoryContainer.Builder {}
    }
}
