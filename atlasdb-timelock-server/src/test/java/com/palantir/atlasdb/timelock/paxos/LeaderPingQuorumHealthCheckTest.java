/**
 * Copyright 2017 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.timelock.paxos;


import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.palantir.leader.PingableLeader;

public class LeaderPingQuorumHealthCheckTest {

    @Test
    public void shouldBeHealthyIfAllNodesPingedSuccessfully() throws Exception {
        PingableLeader leader1 = getMockOfPingableLeaderWherePingReturns(true);
        PingableLeader leader2 = getMockOfPingableLeaderWherePingReturns(true);
        PingableLeader leader3 = getMockOfPingableLeaderWherePingReturns(true);
        ImmutableSet<PingableLeader> leaders = ImmutableSet.of(leader1, leader2, leader3);

        Assertions.assertThat(new LeaderPingQuorumHealthCheck(leaders).check().isHealthy()).isTrue();
    }

    @Test
    public void shouldBeHealthyIfQuorumNodesPingedSuccessfully() throws Exception {
        PingableLeader leader1 = getMockOfPingableLeaderWherePingReturns(true);
        PingableLeader leader2 = getMockOfPingableLeaderWherePingReturns(true);
        PingableLeader leader3 = getMockOfPingableLeaderWherePingReturns(false);
        ImmutableSet<PingableLeader> leaders = ImmutableSet.of(leader1, leader2, leader3);

        Assertions.assertThat(new LeaderPingQuorumHealthCheck(leaders).check().isHealthy()).isTrue();
    }

    @Test
    public void shouldBeUnHealthyIfQuorumNodesNotPingedSuccessfully() throws Exception {
        PingableLeader leader1 = getMockOfPingableLeaderWherePingReturns(true);
        PingableLeader leader2 = getMockOfPingableLeaderWherePingReturns(false);
        PingableLeader leader3 = getMockOfPingableLeaderWherePingReturns(false);
        ImmutableSet<PingableLeader> leaders = ImmutableSet.of(leader1, leader2, leader3);

        Assertions.assertThat(new LeaderPingQuorumHealthCheck(leaders).check().isHealthy()).isFalse();
    }

    private PingableLeader getMockOfPingableLeaderWherePingReturns(boolean pingResult) {
        PingableLeader mockLeader = mock(PingableLeader.class);
        when(mockLeader.ping()).thenReturn(pingResult);
        return mockLeader;
    }
}
