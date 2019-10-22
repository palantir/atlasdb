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
import static org.mockito.Mockito.when;

import java.util.Set;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.ImmutableSet;
import com.palantir.paxos.LeaderPingResults;

@RunWith(MockitoJUnitRunner.class)
public class PingCoalescingFunctionTests {

    private static final Client CLIENT_1 = Client.of("client1");
    private static final Client CLIENT_2 = Client.of("client2");
    private static final Client CLIENT_3 = Client.of("client3");

    @Mock private BatchPingableLeader remote;

    @Test
    public void canProcessBatch() {
        Set<Client> clients = ImmutableSet.of(CLIENT_1, CLIENT_2, CLIENT_3);
        when(remote.ping(clients))
                .thenReturn(ImmutableSet.of(CLIENT_1, CLIENT_3));

        PingCoalescingFunction function = new PingCoalescingFunction(remote);
        assertThat(function.apply(clients))
                .containsEntry(CLIENT_1, LeaderPingResults.pingReturnedTrue())
                .containsEntry(CLIENT_3, LeaderPingResults.pingReturnedTrue())
                .doesNotContainEntry(CLIENT_2, LeaderPingResults.pingReturnedTrue())
                .containsEntry(CLIENT_2, LeaderPingResults.pingReturnedFalse());
    }
}
