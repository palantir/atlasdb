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

import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Test;
import org.mockito.Answers;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.palantir.paxos.ImmutableLeaderPingerContext;
import com.palantir.paxos.LeaderPingResults;
import com.palantir.paxos.LeaderPinger;
import com.palantir.paxos.LeaderPingerContext;

public class AutobatchingPingableLeaderFactoryTests {

    private static final Client CLIENT_1 = Client.of("client-1");
    private static final Client CLIENT_2 = Client.of("client-2");
    private static final URL TIMELOCK_URL = createUrl("https://localhost:8080/timelock/api");

    private final ExecutorService executorService = Executors.newCachedThreadPool();

    @After
    public void after() {
        executorService.shutdown();
    }

    @Test
    public void canPingLeaderMatchingUuidAndClient() {
        LeaderPingerContext<BatchPingableLeader> rpc = batchPingableLeader(TIMELOCK_URL, CLIENT_1);
        AutobatchingPingableLeaderFactory factory = factoryForPingables(rpc);

        LeaderPinger client1Pinger = factory.leaderPingerFor(CLIENT_1);
        LeaderPinger client2Pinger = factory.leaderPingerFor(CLIENT_2);

        assertThat(client1Pinger.pingLeaderWithUuid(rpc.pinger().uuid()))
                .isEqualTo(LeaderPingResults.pingReturnedTrue(rpc.pinger().uuid(), TIMELOCK_URL));

        assertThat(client2Pinger.pingLeaderWithUuid(rpc.pinger().uuid()))
                .isEqualTo(LeaderPingResults.pingReturnedFalse());
    }

    @Test
    public void unknownUuidReturnsFalse() {
        LeaderPingerContext<BatchPingableLeader> rpc = batchPingableLeader(TIMELOCK_URL, CLIENT_1, CLIENT_2);
        AutobatchingPingableLeaderFactory factory = factoryForPingables(rpc);

        LeaderPinger client1Pinger = factory.leaderPingerFor(CLIENT_1);
        LeaderPinger client2Pinger = factory.leaderPingerFor(CLIENT_2);

        assertThat(client1Pinger.pingLeaderWithUuid(UUID.randomUUID()))
                .isEqualTo(LeaderPingResults.pingReturnedFalse());

        assertThat(client2Pinger.pingLeaderWithUuid(UUID.randomUUID()))
                .isEqualTo(LeaderPingResults.pingReturnedFalse());
    }

    @Test
    public void twoDifferentLeaders() {
        URL leader1 = createUrl("https://timelock-1:8080/timelock/api");
        URL leader2 = createUrl("https://timelock-2:8080/timelock/api");
        LeaderPingerContext<BatchPingableLeader> client1Leader =
                batchPingableLeader(leader1, CLIENT_1);
        LeaderPingerContext<BatchPingableLeader> client2Leader =
                batchPingableLeader(leader2, CLIENT_2);

        AutobatchingPingableLeaderFactory factory = factoryForPingables(client1Leader, client2Leader);
        LeaderPinger client1Pinger = factory.leaderPingerFor(CLIENT_1);
        LeaderPinger client2Pinger = factory.leaderPingerFor(CLIENT_2);

        assertThat(client1Pinger.pingLeaderWithUuid(client1Leader.pinger().uuid()))
                .isEqualTo(LeaderPingResults.pingReturnedTrue(client1Leader.pinger().uuid(), leader1));

        assertThat(client2Pinger.pingLeaderWithUuid(client2Leader.pinger().uuid()))
                .isEqualTo(LeaderPingResults.pingReturnedTrue(client2Leader.pinger().uuid(), leader2));
    }

    @Test
    public void pingFailureReturnsFalse() {
        UUID uuid = UUID.randomUUID();
        BatchPingableLeader rpc = mock(BatchPingableLeader.class, Answers.RETURNS_SMART_NULLS);
        when(rpc.uuid()).thenReturn(uuid);
        RuntimeException error = new RuntimeException("ping failure");
        when(rpc.ping(anySet())).thenThrow(error);

        AutobatchingPingableLeaderFactory factory =
                factoryForPingables(ImmutableLeaderPingerContext.of(rpc, TIMELOCK_URL));

        assertThat(factory.leaderPingerFor(CLIENT_1).pingLeaderWithUuid(uuid))
                .isEqualTo(LeaderPingResults.pingCallFailure(error));
        assertThat(factory.leaderPingerFor(CLIENT_2).pingLeaderWithUuid(uuid))
                .isEqualTo(LeaderPingResults.pingCallFailure(error));

        // assert that the rpc call did actually take place!
        verify(rpc).uuid();
        verify(rpc, times(2)).ping(anySet());
    }

    private AutobatchingPingableLeaderFactory factoryForPingables(LeaderPingerContext<BatchPingableLeader>... rpcs) {
        return AutobatchingPingableLeaderFactory.create(
                Maps.toMap(ImmutableSet.copyOf(rpcs), $ -> executorService),
                Duration.ofSeconds(1),
                UUID.randomUUID());
    }

    private static LeaderPingerContext<BatchPingableLeader> batchPingableLeader(
            URL url,
            Client... clientsWhichWeAreLeaderFor) {
        FakeBatchPingableLeader fakeBatchPingableLeader = new FakeBatchPingableLeader(clientsWhichWeAreLeaderFor);
        return ImmutableLeaderPingerContext.of(fakeBatchPingableLeader, url);
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

    private static URL createUrl(String url) {
        try {
            return new URL(url);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }
}
