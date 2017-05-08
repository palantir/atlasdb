/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

import feign.FeignException;

public class LeaderPingHealthCheckTest {
    private static final String MULTIPLE_LEADERS_MESSAGE = "There are multiple leaders in the Paxos cluster.";
    private static final String EXACTLY_ONE_LEADER_MESSAGE = "There is exactly one leader in the Paxos cluster.";
    private static final String NO_LEADER_MESSAGE = "There are no leaders in the Paxos cluster";
    private static final String QUORUM_NODES_DOWN_MESSAGE = "Less than a quorum of nodes responded to a ping request.";

    @Test
    public void shouldBeUnhealthyIfAllNodesPingedSuccessfully() throws Exception {
        ImmutableSet<PingableLeader> leaders = getPingableLeaders(true, true, true);
        assertThat(new LeaderPingHealthCheck(leaders).check().isHealthy()).isFalse();
        assertThat(new LeaderPingHealthCheck(leaders).check().getMessage()).startsWith(MULTIPLE_LEADERS_MESSAGE);
    }

    @Test
    public void shouldBeUnhealthyIfMultipleNodesPingedSuccessfully() throws Exception {
        ImmutableSet<PingableLeader> leaders = getPingableLeaders(true, true, false);
        assertThat(new LeaderPingHealthCheck(leaders).check().isHealthy()).isFalse();
        assertThat(new LeaderPingHealthCheck(leaders).check().getMessage()).startsWith(MULTIPLE_LEADERS_MESSAGE);
    }

    @Test
    public void shouldBeHealthyIfExactlyOneNodePingedSuccessfully() throws Exception {
        ImmutableSet<PingableLeader> leaders = getPingableLeaders(true, false, false);
        assertThat(new LeaderPingHealthCheck(leaders).check().isHealthy()).isTrue();
        assertThat(new LeaderPingHealthCheck(leaders).check().getMessage()).startsWith(EXACTLY_ONE_LEADER_MESSAGE);
    }

    @Test
    public void shouldBeUnhealthyIfNoNodesPingedSuccessfully() throws Exception {
        ImmutableSet<PingableLeader> leaders = getPingableLeaders(false, false, false);
        assertThat(new LeaderPingHealthCheck(leaders).check().isHealthy()).isFalse();
        assertThat(new LeaderPingHealthCheck(leaders).check().getMessage()).startsWith(NO_LEADER_MESSAGE);
    }

    @Test
    public void shouldBeHealthyIfQuorumNodesUpAndOnePingedSuccessfully() throws Exception {
        PingableLeader leader1 = getMockOfPingableLeaderWherePingReturns(true);
        PingableLeader leader2 = getMockOfPingableLeaderWherePingReturns(false);
        PingableLeader leader3 = getMockOfPingableLeaderWherePingThrows();
        ImmutableSet<PingableLeader> leaders = ImmutableSet.of(leader1, leader2, leader3);
        assertThat(new LeaderPingHealthCheck(leaders).check().isHealthy()).isTrue();
        assertThat(new LeaderPingHealthCheck(leaders).check().getMessage()).startsWith(EXACTLY_ONE_LEADER_MESSAGE);
    }

    @Test
    public void shouldBeUnhealthyIfQuorumNodesAreUpAndNoNodePingedSuccessfully() throws Exception {
        PingableLeader leader1 = getMockOfPingableLeaderWherePingReturns(false);
        PingableLeader leader2 = getMockOfPingableLeaderWherePingReturns(false);
        PingableLeader leader3 = getMockOfPingableLeaderWherePingThrows();
        ImmutableSet<PingableLeader> leaders = ImmutableSet.of(leader1, leader2, leader3);
        assertThat(new LeaderPingHealthCheck(leaders).check().isHealthy()).isFalse();
        assertThat(new LeaderPingHealthCheck(leaders).check().getMessage()).startsWith(NO_LEADER_MESSAGE);
    }

    @Test
    public void shouldBeUnhealthyIfQuorumNodesAreNotUpAndOnePingedSuccessfully() throws Exception {
        PingableLeader leader1 = getMockOfPingableLeaderWherePingReturns(true);
        PingableLeader leader2 = getMockOfPingableLeaderWherePingThrows();
        PingableLeader leader3 = getMockOfPingableLeaderWherePingThrows();
        ImmutableSet<PingableLeader> leaders = ImmutableSet.of(leader1, leader2, leader3);
        assertThat(new LeaderPingHealthCheck(leaders).check().isHealthy()).isFalse();
        assertThat(new LeaderPingHealthCheck(leaders).check().getMessage()).startsWith(QUORUM_NODES_DOWN_MESSAGE);
    }

    @Test
    public void shouldBeUnhealthyIfQuorumNodesAreNotUpAndNoNodePingedSuccessfully() throws Exception {
        PingableLeader leader1 = getMockOfPingableLeaderWherePingReturns(false);
        PingableLeader leader2 = getMockOfPingableLeaderWherePingThrows();
        PingableLeader leader3 = getMockOfPingableLeaderWherePingThrows();
        ImmutableSet<PingableLeader> leaders = ImmutableSet.of(leader1, leader2, leader3);
        assertThat(new LeaderPingHealthCheck(leaders).check().isHealthy()).isFalse();
        assertThat(new LeaderPingHealthCheck(leaders).check().getMessage()).startsWith(QUORUM_NODES_DOWN_MESSAGE);
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
        when(mockLeader.ping()).thenThrow(mock(FeignException.class));
        return mockLeader;
    }
}
