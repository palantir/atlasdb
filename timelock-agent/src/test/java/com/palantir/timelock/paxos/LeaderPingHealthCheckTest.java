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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.palantir.paxos.Client;
import com.palantir.timelock.TimeLockStatus;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

public class LeaderPingHealthCheckTest {

    private static final Client CLIENT1 = Client.of("client-1");
    private static final Client CLIENT2 = Client.of("client-2");
    private static final Client CLIENT3 = Client.of("client-3");
    private static final ImmutableSet<Client> ALL_CLIENTS = ImmutableSet.of(CLIENT1, CLIENT2, CLIENT3);
    private static final NamespaceTracker TRACKER = () -> ALL_CLIENTS;

    @Test
    public void shouldBeUnhealthyIfAllNodesPingedSuccessfully() {
        List<HealthCheckPinger> leaders = getHealthCheckPingers(ALL_CLIENTS, ALL_CLIENTS, ALL_CLIENTS);

        SetMultimap<TimeLockStatus, Client> expected = ImmutableSetMultimap.<TimeLockStatus, Client>builder()
                .putAll(TimeLockStatus.MULTIPLE_LEADERS, ALL_CLIENTS)
                .build();

        assertThat(new LeaderPingHealthCheck(TRACKER, leaders).getStatus().statusesToClient())
                .isEqualTo(expected);
    }

    @Test
    public void shouldBeUnhealthyIfMultipleNodesPingedSuccessfully() {
        List<HealthCheckPinger> leaders = getHealthCheckPingers(ALL_CLIENTS, ALL_CLIENTS, ImmutableSet.of());

        SetMultimap<TimeLockStatus, Client> expected = ImmutableSetMultimap.<TimeLockStatus, Client>builder()
                .putAll(TimeLockStatus.MULTIPLE_LEADERS, ALL_CLIENTS)
                .build();

        assertThat(new LeaderPingHealthCheck(TRACKER, leaders).getStatus().statusesToClient())
                .isEqualTo(expected);
    }

    @Test
    public void shouldBeHealthyIfExactlyOneNodePingedSuccessfully() {
        List<HealthCheckPinger> leaders = getHealthCheckPingers(ALL_CLIENTS, ImmutableSet.of(), ImmutableSet.of());

        SetMultimap<TimeLockStatus, Client> expected = ImmutableSetMultimap.<TimeLockStatus, Client>builder()
                .putAll(TimeLockStatus.ONE_LEADER, ALL_CLIENTS)
                .build();

        assertThat(new LeaderPingHealthCheck(TRACKER, leaders).getStatus().statusesToClient())
                .isEqualTo(expected);
    }

    @Test
    public void shouldBeUnhealthyIfNoNodesPingedSuccessfully() {
        List<HealthCheckPinger> leaders =
                getHealthCheckPingers(ImmutableSet.of(), ImmutableSet.of(), ImmutableSet.of());

        SetMultimap<TimeLockStatus, Client> expected = ImmutableSetMultimap.<TimeLockStatus, Client>builder()
                .putAll(TimeLockStatus.NO_LEADER, ALL_CLIENTS)
                .build();

        assertThat(new LeaderPingHealthCheck(TRACKER, leaders).getStatus().statusesToClient())
                .isEqualTo(expected);
    }

    @Test
    public void shouldBeHealthyIfQuorumNodesUpAndOnePingedSuccessfully() {
        HealthCheckPinger leader1 = getMockOfPingableLeaderWherePingReturns(ALL_CLIENTS);
        HealthCheckPinger leader2 = getMockOfPingableLeaderWherePingReturns(ImmutableSet.of());
        HealthCheckPinger leader3 = getMockOfPingableLeaderWherePingThrows();
        List<HealthCheckPinger> leaders = ImmutableList.of(leader1, leader2, leader3);

        SetMultimap<TimeLockStatus, Client> expected = ImmutableSetMultimap.<TimeLockStatus, Client>builder()
                .putAll(TimeLockStatus.ONE_LEADER, ALL_CLIENTS)
                .build();

        assertThat(new LeaderPingHealthCheck(TRACKER, leaders).getStatus().statusesToClient())
                .isEqualTo(expected);
    }

    @Test
    public void shouldBeUnhealthyIfQuorumNodesAreUpAndNoNodePingedSuccessfully() {
        HealthCheckPinger leader1 = getMockOfPingableLeaderWherePingReturns(ImmutableSet.of());
        HealthCheckPinger leader2 = getMockOfPingableLeaderWherePingReturns(ImmutableSet.of());
        HealthCheckPinger leader3 = getMockOfPingableLeaderWherePingThrows();
        List<HealthCheckPinger> leaders = ImmutableList.of(leader1, leader2, leader3);

        SetMultimap<TimeLockStatus, Client> expected = ImmutableSetMultimap.<TimeLockStatus, Client>builder()
                .putAll(TimeLockStatus.NO_LEADER, ALL_CLIENTS)
                .build();

        assertThat(new LeaderPingHealthCheck(TRACKER, leaders).getStatus().statusesToClient())
                .isEqualTo(expected);
    }

    @Test
    public void shouldBeUnhealthyIfQuorumNodesAreNotUpAndOnePingedSuccessfully() {
        HealthCheckPinger leader1 = getMockOfPingableLeaderWherePingReturns(ALL_CLIENTS);
        HealthCheckPinger leader2 = getMockOfPingableLeaderWherePingThrows();
        HealthCheckPinger leader3 = getMockOfPingableLeaderWherePingThrows();
        List<HealthCheckPinger> leaders = ImmutableList.of(leader1, leader2, leader3);

        SetMultimap<TimeLockStatus, Client> expected = ImmutableSetMultimap.<TimeLockStatus, Client>builder()
                .putAll(TimeLockStatus.NO_QUORUM, ALL_CLIENTS)
                .build();

        assertThat(new LeaderPingHealthCheck(TRACKER, leaders).getStatus().statusesToClient())
                .isEqualTo(expected);
    }

    @Test
    public void shouldBeUnhealthyIfQuorumNodesAreNotUpAndNoNodePingedSuccessfully() {
        HealthCheckPinger leader1 = getMockOfPingableLeaderWherePingReturns(ImmutableSet.of());
        HealthCheckPinger leader2 = getMockOfPingableLeaderWherePingThrows();
        HealthCheckPinger leader3 = getMockOfPingableLeaderWherePingThrows();
        List<HealthCheckPinger> leaders = ImmutableList.of(leader1, leader2, leader3);

        SetMultimap<TimeLockStatus, Client> expected = ImmutableSetMultimap.<TimeLockStatus, Client>builder()
                .putAll(TimeLockStatus.NO_QUORUM, ALL_CLIENTS)
                .build();

        assertThat(new LeaderPingHealthCheck(TRACKER, leaders).getStatus().statusesToClient())
                .isEqualTo(expected);
    }

    @Test
    public void canHandleIndividualClientsSeperately() {
        List<HealthCheckPinger> leaders =
                getHealthCheckPingers(ImmutableSet.of(CLIENT1), ImmutableSet.of(CLIENT3), ImmutableSet.of(CLIENT3));

        SetMultimap<TimeLockStatus, Client> expected = ImmutableSetMultimap.<TimeLockStatus, Client>builder()
                .put(TimeLockStatus.ONE_LEADER, CLIENT1)
                .put(TimeLockStatus.NO_LEADER, CLIENT2)
                .put(TimeLockStatus.MULTIPLE_LEADERS, CLIENT3)
                .build();

        assertThat(new LeaderPingHealthCheck(TRACKER, leaders).getStatus().statusesToClient())
                .isEqualTo(expected);
    }

    private static List<HealthCheckPinger> getHealthCheckPingers(
            Set<Client> pingResultForLeader1, Set<Client> pingResultForLeader2, Set<Client> pingResultForLeader3) {
        HealthCheckPinger leader1 = getMockOfPingableLeaderWherePingReturns(pingResultForLeader1);
        HealthCheckPinger leader2 = getMockOfPingableLeaderWherePingReturns(pingResultForLeader2);
        HealthCheckPinger leader3 = getMockOfPingableLeaderWherePingReturns(pingResultForLeader3);
        return ImmutableList.of(leader1, leader2, leader3);
    }

    private static HealthCheckPinger getMockOfPingableLeaderWherePingReturns(Set<Client> pingResult) {
        HealthCheckPinger mockLeader = mock(HealthCheckPinger.class);
        Map<Client, HealthCheckResponse> response =
                Maps.toMap(ALL_CLIENTS, client -> new HealthCheckResponse(pingResult.contains(client)));
        when(mockLeader.apply(ALL_CLIENTS)).thenReturn(response);
        return mockLeader;
    }

    private static HealthCheckPinger getMockOfPingableLeaderWherePingThrows() {
        HealthCheckPinger mockLeader = mock(HealthCheckPinger.class);
        when(mockLeader.apply(ALL_CLIENTS)).thenThrow(new IllegalStateException());
        return mockLeader;
    }
}
