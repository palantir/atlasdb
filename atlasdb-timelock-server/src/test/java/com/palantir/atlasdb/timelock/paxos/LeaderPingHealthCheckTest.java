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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.palantir.leader.PingableLeader;

public class LeaderPingHealthCheckTest {

    @Test
    public void shouldBeUnhealthyIfAllNodesPingedSuccessfully() throws Exception {
        ImmutableSet<PingableLeader> leaders = getPingableLeaders(true, true, true);
        assertThat(new LeaderPingHealthCheck(leaders).check().isHealthy()).isFalse();
    }

    @Test
    public void shouldBeUnhealthyIfMultipleNodesPingedSuccessfully() throws Exception {
        ImmutableSet<PingableLeader> leaders = getPingableLeaders(true, true, false);
        assertThat(new LeaderPingHealthCheck(leaders).check().isHealthy()).isFalse();
    }

    @Test
    public void shouldBeHealthyIfExactlyOneNodePingedSuccessfully() throws Exception {
        ImmutableSet<PingableLeader> leaders = getPingableLeaders(true, false, false);
        assertThat(new LeaderPingHealthCheck(leaders).check().isHealthy()).isTrue();
    }

    @Test
    public void shouldBeUnhealthyIfNoNodesPingedSuccessfully() throws Exception {
        ImmutableSet<PingableLeader> leaders = getPingableLeaders(false, false, false);
        assertThat(new LeaderPingHealthCheck(leaders).check().isHealthy()).isFalse();
    }


    private ImmutableSet<PingableLeader> getPingableLeaders(
            boolean pingResultForLeader1,
            boolean pingResultForLeader2,
            boolean pingResultForLeader3) {
        PingableLeader leader1 = getMockOfPingableLeaderWherePingReturns(pingResultForLeader1);
        PingableLeader leader2 = getMockOfPingableLeaderWherePingReturns(pingResultForLeader2);
        PingableLeader leader3 = getMockOfPingableLeaderWherePingReturns(pingResultForLeader3);
        return ImmutableSet.of(leader1, leader2, leader3);
    }

    private PingableLeader getMockOfPingableLeaderWherePingReturns(boolean pingResult) {
        PingableLeader mockLeader = mock(PingableLeader.class);
        when(mockLeader.ping()).thenReturn(pingResult);
        return mockLeader;
    }
}
