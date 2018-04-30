/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.timelock.paxos;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.http.errors.AtlasDbRemoteException;
import com.palantir.leader.PingableLeader;
import com.palantir.timelock.TimeLockStatus;

public class LeaderPingHealthCheckTest {

    @Test
    public void shouldBeUnhealthyIfAllNodesPingedSuccessfully() throws Exception {
        ImmutableSet<PingableLeader> leaders = getPingableLeaders(true, true, true);
        assertEquals(TimeLockStatus.MULTIPLE_LEADERS, new LeaderPingHealthCheck(leaders).getStatus());
    }

    @Test
    public void shouldBeUnhealthyIfMultipleNodesPingedSuccessfully() throws Exception {
        ImmutableSet<PingableLeader> leaders = getPingableLeaders(true, true, false);
        assertEquals(TimeLockStatus.MULTIPLE_LEADERS, new LeaderPingHealthCheck(leaders).getStatus());
    }

    @Test
    public void shouldBeHealthyIfExactlyOneNodePingedSuccessfully() throws Exception {
        ImmutableSet<PingableLeader> leaders = getPingableLeaders(true, false, false);
        assertEquals(TimeLockStatus.ONE_LEADER, new LeaderPingHealthCheck(leaders).getStatus());
    }

    @Test
    public void shouldBeUnhealthyIfNoNodesPingedSuccessfully() throws Exception {
        ImmutableSet<PingableLeader> leaders = getPingableLeaders(false, false, false);
        assertEquals(TimeLockStatus.NO_LEADER, new LeaderPingHealthCheck(leaders).getStatus());
    }

    @Test
    public void shouldBeHealthyIfQuorumNodesUpAndOnePingedSuccessfully() throws Exception {
        PingableLeader leader1 = getMockOfPingableLeaderWherePingReturns(true);
        PingableLeader leader2 = getMockOfPingableLeaderWherePingReturns(false);
        PingableLeader leader3 = getMockOfPingableLeaderWherePingThrows();
        ImmutableSet<PingableLeader> leaders = ImmutableSet.of(leader1, leader2, leader3);
        assertEquals(TimeLockStatus.ONE_LEADER, new LeaderPingHealthCheck(leaders).getStatus());
    }

    @Test
    public void shouldBeUnhealthyIfQuorumNodesAreUpAndNoNodePingedSuccessfully() throws Exception {
        PingableLeader leader1 = getMockOfPingableLeaderWherePingReturns(false);
        PingableLeader leader2 = getMockOfPingableLeaderWherePingReturns(false);
        PingableLeader leader3 = getMockOfPingableLeaderWherePingThrows();
        ImmutableSet<PingableLeader> leaders = ImmutableSet.of(leader1, leader2, leader3);
        assertEquals(TimeLockStatus.NO_LEADER, new LeaderPingHealthCheck(leaders).getStatus());
    }

    @Test
    public void shouldBeUnhealthyIfQuorumNodesAreNotUpAndOnePingedSuccessfully() throws Exception {
        PingableLeader leader1 = getMockOfPingableLeaderWherePingReturns(true);
        PingableLeader leader2 = getMockOfPingableLeaderWherePingThrows();
        PingableLeader leader3 = getMockOfPingableLeaderWherePingThrows();
        ImmutableSet<PingableLeader> leaders = ImmutableSet.of(leader1, leader2, leader3);
        assertEquals(TimeLockStatus.NO_QUORUM, new LeaderPingHealthCheck(leaders).getStatus());
    }

    @Test
    public void shouldBeUnhealthyIfQuorumNodesAreNotUpAndNoNodePingedSuccessfully() throws Exception {
        PingableLeader leader1 = getMockOfPingableLeaderWherePingReturns(false);
        PingableLeader leader2 = getMockOfPingableLeaderWherePingThrows();
        PingableLeader leader3 = getMockOfPingableLeaderWherePingThrows();
        ImmutableSet<PingableLeader> leaders = ImmutableSet.of(leader1, leader2, leader3);
        assertEquals(TimeLockStatus.NO_QUORUM, new LeaderPingHealthCheck(leaders).getStatus());
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

    private PingableLeader getMockOfPingableLeaderWherePingThrows() {
        PingableLeader mockLeader = mock(PingableLeader.class);
        when(mockLeader.ping()).thenThrow(mock(AtlasDbRemoteException.class));
        return mockLeader;
    }
}
