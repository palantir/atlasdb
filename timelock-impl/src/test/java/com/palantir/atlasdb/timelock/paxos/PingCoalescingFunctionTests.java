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

import static com.palantir.paxos.LeaderPingResults.pingReturnedFalse;
import static com.palantir.paxos.LeaderPingResults.pingReturnedTrue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import com.palantir.atlasdb.timelock.paxos.AutobatchingPingableLeaderFactory.PingRequest;
import com.palantir.paxos.Client;
import com.palantir.paxos.ImmutableLeaderPingerContext;
import java.util.Set;
import java.util.UUID;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PingCoalescingFunctionTests {

    private static final Client CLIENT_1 = Client.of("client1");
    private static final Client CLIENT_2 = Client.of("client2");
    private static final Client CLIENT_3 = Client.of("client3");

    private static final UUID LEADER_UUID_1 = UUID.randomUUID();
    private static final UUID LEADER_UUID_2 = UUID.randomUUID();

    private static final HostAndPort HOST_AND_PORT = HostAndPort.fromParts("localhost", 8080);

    @Mock
    private BatchPingableLeader remote;

    @Test
    public void canProcessBatch() {
        Set<Client> clients = ImmutableSet.of(CLIENT_1, CLIENT_2, CLIENT_3);
        when(remote.ping(clients)).thenReturn(ImmutableSet.of(CLIENT_1, CLIENT_3));

        Set<PingRequest> requests = ImmutableSet.of(
                pingRequest(CLIENT_1, LEADER_UUID_1),
                pingRequest(CLIENT_2, LEADER_UUID_2),
                pingRequest(CLIENT_3, LEADER_UUID_1));

        PingCoalescingFunction function =
                new PingCoalescingFunction(ImmutableLeaderPingerContext.of(remote, HOST_AND_PORT));
        assertThat(function.apply(requests))
                .containsEntry(pingRequest(CLIENT_1, LEADER_UUID_1), pingReturnedTrue(LEADER_UUID_1, HOST_AND_PORT))
                .containsEntry(pingRequest(CLIENT_3, LEADER_UUID_1), pingReturnedTrue(LEADER_UUID_1, HOST_AND_PORT))
                .doesNotContainEntry(
                        pingRequest(CLIENT_2, LEADER_UUID_2), pingReturnedTrue(LEADER_UUID_2, HOST_AND_PORT))
                .containsEntry(pingRequest(CLIENT_2, LEADER_UUID_2), pingReturnedFalse());
    }

    private static PingRequest pingRequest(Client client, UUID leaderUuid) {
        return ImmutablePingRequest.of(client, leaderUuid);
    }
}
