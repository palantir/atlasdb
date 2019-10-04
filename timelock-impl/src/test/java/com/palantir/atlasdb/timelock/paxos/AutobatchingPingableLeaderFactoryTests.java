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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.palantir.paxos.LeaderPinger;

public class AutobatchingPingableLeaderFactoryTests {

    private static final Client CLIENT_1 = Client.of("client-1");
    private static final Client CLIENT_2 = Client.of("client-2");
    private final ExecutorService executorService = Executors.newCachedThreadPool();

    @After
    public void after() {
        executorService.shutdown();
    }

    @Test
    public void canPingLeaderMatchingUuidAndClient() {
        BatchPingableLeader rpc = new FakeBatchPingableLeader(CLIENT_1);
        AutobatchingPingableLeaderFactory factory = factoryForPingables(rpc);

        LeaderPinger client1Pinger = factory.leaderPingerFor(CLIENT_1);
        LeaderPinger client2Pinger = factory.leaderPingerFor(CLIENT_2);

        assertThat(client1Pinger.pingLeaderWithUuid(rpc.uuid()))
                .isTrue();

        assertThat(client2Pinger.pingLeaderWithUuid(rpc.uuid()))
                .isFalse();
    }

    @Test
    public void unknownUuidReturnsFalse() {
        BatchPingableLeader rpc = new FakeBatchPingableLeader(CLIENT_1, CLIENT_2);
        AutobatchingPingableLeaderFactory factory = factoryForPingables(rpc);

        LeaderPinger client1Pinger = factory.leaderPingerFor(CLIENT_1);
        LeaderPinger client2Pinger = factory.leaderPingerFor(CLIENT_2);

        assertThat(client1Pinger.pingLeaderWithUuid(UUID.randomUUID()))
                .isFalse();

        assertThat(client2Pinger.pingLeaderWithUuid(UUID.randomUUID()))
                .isFalse();
    }

    @Test
    public void twoDifferentLeaders() {
        FakeBatchPingableLeader client1Leader = new FakeBatchPingableLeader(CLIENT_1);
        FakeBatchPingableLeader client2Leader = new FakeBatchPingableLeader(CLIENT_2);

        AutobatchingPingableLeaderFactory factory = factoryForPingables(client1Leader, client2Leader);
        LeaderPinger client1Pinger = factory.leaderPingerFor(CLIENT_1);
        LeaderPinger client2Pinger = factory.leaderPingerFor(CLIENT_2);

        assertThat(client1Pinger.pingLeaderWithUuid(client1Leader.uuid))
                .isTrue();

        assertThat(client2Pinger.pingLeaderWithUuid(client2Leader.uuid))
                .isTrue();
    }

    @Test
    public void pingFailureReturnsFalse() {
        UUID uuid = UUID.randomUUID();
        BatchPingableLeader rpc = mock(BatchPingableLeader.class);
        when(rpc.uuid()).thenReturn(uuid);
        when(rpc.ping(anySet())).thenThrow(new RuntimeException("ping failure"));

        AutobatchingPingableLeaderFactory factory = factoryForPingables(rpc);

        assertThat(factory.leaderPingerFor(CLIENT_1).pingLeaderWithUuid(uuid))
                .isFalse();
        assertThat(factory.leaderPingerFor(CLIENT_2).pingLeaderWithUuid(uuid))
                .isFalse();

        // assert that the rpc call did actually take place!
        verify(rpc).uuid();
        verify(rpc, times(2)).ping(anySet());
    }

    private AutobatchingPingableLeaderFactory factoryForPingables(BatchPingableLeader... rpcs) {
        return AutobatchingPingableLeaderFactory.create(
                Maps.toMap(ImmutableSet.copyOf(rpcs), $ -> executorService),
                Duration.ofSeconds(1),
                UUID.randomUUID());
    }

    private static final class FakeBatchPingableLeader implements BatchPingableLeader {

        private final Set<Client> clientsWhichWeAreLeaderFor;
        private final UUID uuid = UUID.randomUUID();

        private FakeBatchPingableLeader(Client... clientsWhichWeAreLeaderFor) {
            this.clientsWhichWeAreLeaderFor = ImmutableSet.copyOf(clientsWhichWeAreLeaderFor);
        }

        @Override
        public Set<Client> ping(Set<Client> clients) {
            return Sets.intersection(clientsWhichWeAreLeaderFor, clients);
        }

        @Override
        public UUID uuid() {
            return uuid;
        }
    }
}
