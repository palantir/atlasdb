/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.timelock.paxos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.palantir.leader.PingableLeader;
import com.palantir.timelock.TimeLockStatus;

public class LeaderPingHealthCheckTest {

    @Test
    public void shouldBeUnhealthyIfAllNodesPingedSuccessfully() {
        ImmutableSet<PingableLeader> leaders = getPingableLeaders(true, true, true);
        assertThat(new LeaderPingHealthCheck(leaders).getStatus()).isEqualTo(TimeLockStatus.MULTIPLE_LEADERS);
    }

    @Test
    public void shouldBeUnhealthyIfMultipleNodesPingedSuccessfully() {
        ImmutableSet<PingableLeader> leaders = getPingableLeaders(true, true, false);
        assertThat(new LeaderPingHealthCheck(leaders).getStatus()).isEqualTo(TimeLockStatus.MULTIPLE_LEADERS);
    }

    @Test
    public void shouldBeHealthyIfExactlyOneNodePingedSuccessfully() {
        ImmutableSet<PingableLeader> leaders = getPingableLeaders(true, false, false);
        assertThat(new LeaderPingHealthCheck(leaders).getStatus()).isEqualTo(TimeLockStatus.ONE_LEADER);
    }

    @Test
    public void shouldBeUnhealthyIfNoNodesPingedSuccessfully() {
        ImmutableSet<PingableLeader> leaders = getPingableLeaders(false, false, false);
        assertThat(new LeaderPingHealthCheck(leaders).getStatus()).isEqualTo(TimeLockStatus.NO_LEADER);
    }

    @Test
    public void shouldBeHealthyIfQuorumNodesUpAndOnePingedSuccessfully() {
        PingableLeader leader1 = getMockOfPingableLeaderWherePingReturns(true);
        PingableLeader leader2 = getMockOfPingableLeaderWherePingReturns(false);
        PingableLeader leader3 = getMockOfPingableLeaderWherePingThrows();
        ImmutableSet<PingableLeader> leaders = ImmutableSet.of(leader1, leader2, leader3);
        assertThat(new LeaderPingHealthCheck(leaders).getStatus()).isEqualTo(TimeLockStatus.ONE_LEADER);
    }

    @Test
    public void shouldBeUnhealthyIfQuorumNodesAreUpAndNoNodePingedSuccessfully() {
        PingableLeader leader1 = getMockOfPingableLeaderWherePingReturns(false);
        PingableLeader leader2 = getMockOfPingableLeaderWherePingReturns(false);
        PingableLeader leader3 = getMockOfPingableLeaderWherePingThrows();
        ImmutableSet<PingableLeader> leaders = ImmutableSet.of(leader1, leader2, leader3);
        assertThat(new LeaderPingHealthCheck(leaders).getStatus()).isEqualTo(TimeLockStatus.NO_LEADER);
    }

    @Test
    public void shouldBeUnhealthyIfQuorumNodesAreNotUpAndOnePingedSuccessfully() {
        PingableLeader leader1 = getMockOfPingableLeaderWherePingReturns(true);
        PingableLeader leader2 = getMockOfPingableLeaderWherePingThrows();
        PingableLeader leader3 = getMockOfPingableLeaderWherePingThrows();
        ImmutableSet<PingableLeader> leaders = ImmutableSet.of(leader1, leader2, leader3);
        assertThat(new LeaderPingHealthCheck(leaders).getStatus()).isEqualTo(TimeLockStatus.NO_QUORUM);
    }

    @Test
    public void shouldBeUnhealthyIfQuorumNodesAreNotUpAndNoNodePingedSuccessfully() {
        PingableLeader leader1 = getMockOfPingableLeaderWherePingReturns(false);
        PingableLeader leader2 = getMockOfPingableLeaderWherePingThrows();
        PingableLeader leader3 = getMockOfPingableLeaderWherePingThrows();
        ImmutableSet<PingableLeader> leaders = ImmutableSet.of(leader1, leader2, leader3);
        assertThat(new LeaderPingHealthCheck(leaders).getStatus()).isEqualTo(TimeLockStatus.NO_QUORUM);
    }

    private static ImmutableSet<PingableLeader> getPingableLeaders(
            boolean pingResultForLeader1,
            boolean pingResultForLeader2,
            boolean pingResultForLeader3) {
        PingableLeader leader1 = getMockOfPingableLeaderWherePingReturns(pingResultForLeader1);
        PingableLeader leader2 = getMockOfPingableLeaderWherePingReturns(pingResultForLeader2);
        PingableLeader leader3 = getMockOfPingableLeaderWherePingReturns(pingResultForLeader3);
        return ImmutableSet.of(leader1, leader2, leader3);
    }

    private static PingableLeader getMockOfPingableLeaderWherePingReturns(boolean pingResult) {
        PingableLeader mockLeader = mock(PingableLeader.class);
        when(mockLeader.ping()).thenReturn(pingResult);
        return mockLeader;
    }

    private static PingableLeader getMockOfPingableLeaderWherePingThrows() {
        PingableLeader mockLeader = mock(PingableLeader.class);
        // TODO(gmaretic): fix, used to be AtlasDbRemoteException
        when(mockLeader.ping()).thenThrow(mock(RuntimeException.class));
        return mockLeader;
    }
}
