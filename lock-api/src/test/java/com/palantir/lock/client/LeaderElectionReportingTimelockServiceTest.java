/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsRequest;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsRequest;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsResponse;
import com.palantir.common.time.Clock;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.immutables.value.Value;
import org.immutables.value.Value.Parameter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.OngoingStubbing;

public class LeaderElectionReportingTimelockServiceTest {
    private static final UUID LEADER_1 = UUID.randomUUID();
    private static final UUID LEADER_2 = UUID.randomUUID();
    private static final LockWatchStateUpdate UPDATE = LockWatchStateUpdate.success(LEADER_1, -1L, ImmutableList.of());

    private ConjureStartTransactionsRequest startTransactionsRequest = mock(ConjureStartTransactionsRequest.class);
    private ConjureStartTransactionsResponse startTransactionsResponse = mock(ConjureStartTransactionsResponse.class);
    private GetCommitTimestampsRequest commitTimestampsRequest = mock(GetCommitTimestampsRequest.class);
    private GetCommitTimestampsResponse commitTimestampsResponse = mock(GetCommitTimestampsResponse.class);
    private NamespacedConjureTimelockService mockedDelegate = mock(NamespacedConjureTimelockService.class);
    private TaggedMetricRegistry mockedRegistry = mock(TaggedMetricRegistry.class);
    private Timer mockedTimer = mock(Timer.class);
    private Clock mockedClock = mock(Clock.class);

    private LeaderElectionReportingTimelockService timelockService;

    @Before
    public void before() {
        timelockService = new LeaderElectionReportingTimelockService(mockedDelegate, mockedRegistry, mockedClock);
        when(mockedDelegate.startTransactions(any())).thenReturn(startTransactionsResponse);
        when(mockedDelegate.getCommitTimestamps(any())).thenReturn(commitTimestampsResponse);
        when(mockedRegistry.timer(any())).thenReturn(mockedTimer);
        when(mockedClock.instant()).thenCallRealMethod();
        when(mockedClock.getTimeMillis()).thenReturn(1L);

        when(startTransactionsResponse.getLockWatchUpdate()).thenReturn(UPDATE);
        when(commitTimestampsResponse.getLockWatchUpdate()).thenReturn(UPDATE);
    }

    @Test
    public void firstCallDoesNotReportMetrics() {
        timelockService.startTransactions(startTransactionsRequest);
        verifyNoInteractions(mockedRegistry);
    }

    @Test
    public void sameLeaderDoesNotReportMetrics() {
        timelockService.startTransactions(startTransactionsRequest);
        timelockService.startTransactions(startTransactionsRequest);
        verifyNoInteractions(mockedRegistry);
    }

    @Test
    public void reportMetricsOnLeaderElection() {
        LockWatchStateUpdate.Snapshot secondUpdate =
                LockWatchStateUpdate.snapshot(UUID.randomUUID(), 1L, ImmutableSet.of(), ImmutableSet.of());
        when(startTransactionsResponse.getLockWatchUpdate())
                .thenReturn(UPDATE)
                .thenReturn(secondUpdate)
                .thenReturn(secondUpdate)
                .thenReturn(LockWatchStateUpdate.success(UUID.randomUUID(), 5L, ImmutableList.of()));
        timelockService.startTransactions(startTransactionsRequest);
        timelockService.startTransactions(startTransactionsRequest);
        timelockService.startTransactions(startTransactionsRequest);
        timelockService.startTransactions(startTransactionsRequest);
        verify(mockedRegistry, atLeast(1)).timer(any());
        verify(mockedTimer, times(2)).update(anyLong(), any());
    }

    @Test
    public void leaderIsInitialisedForAllMethods() {
        when(commitTimestampsResponse.getLockWatchUpdate())
                .thenReturn(LockWatchStateUpdate.success(UUID.randomUUID(), 3L, ImmutableList.of()));
        timelockService.getCommitTimestamps(commitTimestampsRequest);
        timelockService.startTransactions(startTransactionsRequest);
        verify(mockedRegistry).timer(any());
        verify(mockedTimer).update(anyLong(), any());
    }

    @Test
    public void noLeaderElectionDurationBeforeLeaderElection() {
        assertThat(timelockService.calculateLastLeaderElectionDuration()).isNotPresent();

        timelockService.startTransactions(startTransactionsRequest);
        assertThat(timelockService.calculateLastLeaderElectionDuration()).isNotPresent();

        timelockService.getCommitTimestamps(commitTimestampsRequest);
        assertThat(timelockService.calculateLastLeaderElectionDuration()).isNotPresent();
    }

    @Test
    public void detectLeaderElectionWithFreshLeader() {
        executeCalls(
                ImmutableSingleCall.of(0L, 1L, LEADER_1),
                ImmutableSingleCall.of(3L, 6L, LEADER_1),
                ImmutableSingleCall.of(10L, 15L, LEADER_2));

        assertExpectedDuration(Instant.ofEpochMilli(3L), Instant.ofEpochMilli(15L));
    }

    @Test
    public void detectLeaderElectionWithFreshLeaderOverlappingRequests() {
        executeCalls(
                ImmutableSingleCall.of(0L, 1L, LEADER_1),
                ImmutableSingleCall.of(6L, 10L, LEADER_1),
                ImmutableSingleCall.of(3L, 15L, LEADER_1),
                ImmutableSingleCall.of(14L, 21L, LEADER_2));

        assertExpectedDuration(Instant.ofEpochMilli(6L), Instant.ofEpochMilli(21L));
    }

    @Test
    public void detectLeaderElectionWithTwoLongTermLeaders() {
        executeCalls(
                ImmutableSingleCall.of(0L, 1L, LEADER_1),
                ImmutableSingleCall.of(3L, 6L, LEADER_1),
                ImmutableSingleCall.of(10L, 15L, LEADER_2),
                ImmutableSingleCall.of(21L, 28L, LEADER_2));

        assertExpectedDuration(Instant.ofEpochMilli(3L), Instant.ofEpochMilli(15L));
    }

    @Test
    public void detectFreshLeaderElectionWithTwoLongTurnLeaders() {
        UUID leader3 = UUID.randomUUID();
        executeCalls(
                ImmutableSingleCall.of(0L, 1L, LEADER_1),
                ImmutableSingleCall.of(3L, 6L, LEADER_1),
                ImmutableSingleCall.of(10L, 15L, LEADER_2),
                ImmutableSingleCall.of(21L, 28L, LEADER_2),
                ImmutableSingleCall.of(36L, 45L, leader3));

        assertExpectedDuration(Instant.ofEpochMilli(21L), Instant.ofEpochMilli(45L));
    }

    private void executeCalls(SingleCall firstCall, SingleCall... otherCalls) {
        LockWatchStateUpdate updateMock = mock(LockWatchStateUpdate.class);
        when(startTransactionsResponse.getLockWatchUpdate()).thenReturn(updateMock);

        OngoingStubbing<Long> clockStubbing = when(mockedClock.getTimeMillis())
                .thenReturn(firstCall.requestMillis())
                .thenReturn(firstCall.responseMillis());
        OngoingStubbing<UUID> updateStubbing = when(updateMock.logId()).thenReturn(firstCall.responseLeader());
        for (SingleCall call : otherCalls) {
            clockStubbing = clockStubbing.thenReturn(call.requestMillis()).thenReturn(call.responseMillis());
            updateStubbing = updateStubbing.thenReturn(call.responseLeader());
        }

        for (int i = 0; i <= otherCalls.length; i++) {
            timelockService.startTransactions(startTransactionsRequest);
        }
    }

    private void assertExpectedDuration(Instant instant, Instant instant2) {
        Optional<Duration> estimatedDuration = timelockService.calculateLastLeaderElectionDuration();
        assertThat(estimatedDuration).isPresent();
        assertThat(estimatedDuration.get()).isEqualTo(Duration.between(instant, instant2));
    }

    @Value.Immutable
    interface SingleCall {
        @Parameter
        long requestMillis();

        @Parameter
        long responseMillis();

        @Parameter
        UUID responseLeader();
    }
}
