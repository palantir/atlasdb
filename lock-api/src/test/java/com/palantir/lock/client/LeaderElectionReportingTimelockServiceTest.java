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
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksRequest;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksResponse;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsRequest;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsRequest;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsResponse;
import com.palantir.common.time.Clock;
import com.palantir.common.time.NanoTime;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.LeadershipId;
import com.palantir.lock.v2.Lease;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.timelock.feedback.LeaderElectionDuration;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.immutables.value.Value;
import org.immutables.value.Value.Parameter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.OngoingStubbing;

public class LeaderElectionReportingTimelockServiceTest {
    private static final LeaderTime LEADER_1_TIME = LeaderTime.of(LeadershipId.random(), NanoTime.createForTests(999L));
    private static final UUID LEADER_1 = LEADER_1_TIME.id().id();
    private static final UUID LEADER_2 = UUID.randomUUID();
    private static final UUID LEADER_3 = UUID.randomUUID();
    private static final LockWatchStateUpdate UPDATE = LockWatchStateUpdate.success(LEADER_1, -1L, ImmutableList.of());
    private static final Lease LEASE = Lease.of(LEADER_1_TIME, Duration.ZERO);

    private ConjureStartTransactionsRequest startTransactionsRequest = mock(ConjureStartTransactionsRequest.class);
    private ConjureStartTransactionsResponse startTransactionsResponse = mock(ConjureStartTransactionsResponse.class);
    private ConjureRefreshLocksRequest refreshLocksRequest = mock(ConjureRefreshLocksRequest.class);
    private ConjureRefreshLocksResponse refreshLocksResponse = mock(ConjureRefreshLocksResponse.class);
    private GetCommitTimestampsRequest commitTimestampsRequest = mock(GetCommitTimestampsRequest.class);
    private GetCommitTimestampsResponse commitTimestampsResponse = mock(GetCommitTimestampsResponse.class);
    private NamespacedConjureTimelockService mockedDelegate = mock(NamespacedConjureTimelockService.class);
    private TaggedMetricRegistry mockedRegistry = mock(TaggedMetricRegistry.class);
    private Timer mockedTimer = mock(Timer.class);
    private Clock mockedClock = mock(Clock.class);
    private com.codahale.metrics.Snapshot mockedSnapshot = mock(com.codahale.metrics.Snapshot.class);

    private LeaderElectionReportingTimelockService timelockService;

    @Before
    public void before() {
        timelockService = new LeaderElectionReportingTimelockService(mockedDelegate, mockedRegistry, mockedClock);
        when(mockedDelegate.startTransactions(any())).thenReturn(startTransactionsResponse);
        when(mockedDelegate.getCommitTimestamps(any())).thenReturn(commitTimestampsResponse);
        when(mockedDelegate.leaderTime()).thenReturn(LEADER_1_TIME);
        when(mockedDelegate.refreshLocks(refreshLocksRequest)).thenReturn(refreshLocksResponse);
        when(mockedRegistry.timer(any())).thenReturn(mockedTimer);
        when(mockedClock.instant()).thenCallRealMethod();
        when(mockedClock.getTimeMillis()).thenReturn(1L);
        when(mockedTimer.getSnapshot()).thenReturn(mockedSnapshot);

        when(startTransactionsResponse.getLockWatchUpdate()).thenReturn(UPDATE);
        when(commitTimestampsResponse.getLockWatchUpdate()).thenReturn(UPDATE);
        when(refreshLocksResponse.getLease()).thenReturn(LEASE);
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
    public void verifyLeaderTimeRunsWithTiming() {
        verifyEndpointRunsWithTiming(timelockService::leaderTime);
    }

    @Test
    public void verifyRefreshLocksRunsWithTiming() {
        verifyEndpointRunsWithTiming(() -> timelockService.refreshLocks(refreshLocksRequest));
    }

    @Test
    public void verifyStartTransactionsRunsWithTiming() {
        verifyEndpointRunsWithTiming(() -> timelockService.startTransactions(startTransactionsRequest));
    }

    @Test
    public void verifyGetCommitTimestampsRunsWithTiming() {
        verifyEndpointRunsWithTiming(() -> timelockService.getCommitTimestamps(commitTimestampsRequest));
    }

    /**
     * [ A ]
     *       [ A ]
     *             [ B ]
     *       <=========>
     */
    @Test
    public void detectLeaderElectionWithFreshLeader() {
        executeCalls(
                ImmutableSingleCall.of(0L, 1L, LEADER_1),
                ImmutableSingleCall.of(3L, 6L, LEADER_1),
                ImmutableSingleCall.of(10L, 15L, LEADER_2));
        assertExpectedDurationAndLeaders(Instant.ofEpochMilli(3L), Instant.ofEpochMilli(15L), LEADER_1, LEADER_2);
    }

    /**
     * [ A ]
     *       [  A  ]
     *           [ B ]
     *       <=======>
     */
    @Test
    public void detectLeaderElectionWithSimpleOverlap() {
        executeCalls(
                ImmutableSingleCall.of(0L, 1L, LEADER_1),
                ImmutableSingleCall.of(3L, 10L, LEADER_1),
                ImmutableSingleCall.of(6L, 15L, LEADER_2));
        assertExpectedDurationAndLeaders(Instant.ofEpochMilli(3L), Instant.ofEpochMilli(15L), LEADER_1, LEADER_2);
    }

    /**
     * [ A ]
     *           [ A ]
     *       [     A     ]
     *                  [ B ]
     *           <==========>
     */
    @Test
    public void detectLeaderElectionWithFreshLeaderOverlappingRequests() {
        executeCalls(
                ImmutableSingleCall.of(0L, 1L, LEADER_1),
                ImmutableSingleCall.of(6L, 10L, LEADER_1),
                ImmutableSingleCall.of(3L, 15L, LEADER_1),
                ImmutableSingleCall.of(14L, 21L, LEADER_2));
        assertExpectedDurationAndLeaders(Instant.ofEpochMilli(6L), Instant.ofEpochMilli(21L), LEADER_1, LEADER_2);
    }

    /**
     * [ A ]
     *       [ A ]
     *             [ B ]
     *                   [ B ]
     *       <=========>
     */
    @Test
    public void detectLeaderElectionWithTwoLongTermLeaders() {
        executeCalls(
                ImmutableSingleCall.of(0L, 1L, LEADER_1),
                ImmutableSingleCall.of(3L, 6L, LEADER_1),
                ImmutableSingleCall.of(10L, 15L, LEADER_2),
                ImmutableSingleCall.of(21L, 28L, LEADER_2));
        assertExpectedDurationAndLeaders(Instant.ofEpochMilli(3L), Instant.ofEpochMilli(15L), LEADER_1, LEADER_2);
    }

    /**
     * [ A ]
     *       [ A ]
     *             [ B ]
     *                   [ B ]
     *                         [ C ]
     *                   <=========>
     */
    @Test
    public void detectFreshLeaderElectionWithTwoLongTermLeaders() {
        executeCalls(
                ImmutableSingleCall.of(0L, 1L, LEADER_1),
                ImmutableSingleCall.of(3L, 6L, LEADER_1),
                ImmutableSingleCall.of(10L, 15L, LEADER_2),
                ImmutableSingleCall.of(21L, 28L, LEADER_2),
                ImmutableSingleCall.of(36L, 45L, LEADER_3));
        assertExpectedDurationAndLeaders(Instant.ofEpochMilli(21L), Instant.ofEpochMilli(45L), LEADER_2, LEADER_3);
    }

    /**
     * [ A ]
     *       [ A ]
     *                [ B ]
     *             [    A    ]
     *             <======>
     */
    @Test
    public void updateOldLeaderUpperBoundWithNewRequests() {
        executeCalls(
                ImmutableSingleCall.of(0L, 1L, LEADER_1),
                ImmutableSingleCall.of(3L, 6L, LEADER_1),
                ImmutableSingleCall.of(15L, 21L, LEADER_2),
                ImmutableSingleCall.of(10L, 28L, LEADER_1));
        assertExpectedDurationAndLeaders(Instant.ofEpochMilli(10L), Instant.ofEpochMilli(21L), LEADER_1, LEADER_2);
    }

    /**
     * [ A ]
     *       [ A ]
     *             [ B ]
     *                   [ C ]
     *                         [ C ]
     *       <=========>
     */
    @Test
    public void detectFirstLeaderElectionFromLongTermLeader() {
        executeCalls(
                ImmutableSingleCall.of(0L, 1L, LEADER_1),
                ImmutableSingleCall.of(3L, 6L, LEADER_1),
                ImmutableSingleCall.of(10L, 15L, LEADER_2),
                ImmutableSingleCall.of(21L, 28L, LEADER_3),
                ImmutableSingleCall.of(36L, 45L, LEADER_3));
        assertExpectedDurationAndLeaders(Instant.ofEpochMilli(3L), Instant.ofEpochMilli(15L), LEADER_1, LEADER_2);
    }

    /**
     * [ A ]
     *       [ A ]
     *             [       B       ]
     *                [ C ]
     *                      [ C ]
     *       <============>
     */
    @Test
    public void detectFirstLeaderElectionFromLongTermLeaderWithSlowRequest() {
        executeCalls(
                ImmutableSingleCall.of(0L, 1L, LEADER_1),
                ImmutableSingleCall.of(3L, 6L, LEADER_1),
                ImmutableSingleCall.of(10L, 45L, LEADER_2),
                ImmutableSingleCall.of(15L, 21L, LEADER_3),
                ImmutableSingleCall.of(28L, 36L, LEADER_3));
        assertExpectedDurationAndLeaders(Instant.ofEpochMilli(3L), Instant.ofEpochMilli(21L), LEADER_1, LEADER_3);
    }

    /**
     * [ A ]
     *       [        B       ]
     *          [ C ]
     *                 [ C ]
     */
    @Test
    public void doNotCalculateLeadershipBeforeFirstLongTermLeader() {
        executeCalls(
                ImmutableSingleCall.of(0L, 1L, LEADER_1),
                ImmutableSingleCall.of(3L, 36L, LEADER_2),
                ImmutableSingleCall.of(10L, 15L, LEADER_3),
                ImmutableSingleCall.of(21L, 28L, LEADER_3));
        assertThat(timelockService.calculateLastLeaderElectionDuration()).isEmpty();
    }

    /**
     * [ A ]
     *       [        A       ]
     *          [ C ]
     *                 [ C ]
     *       <======>
     */
    @Test
    public void detectWhenOldLeaderBecomesLongTerm() {
        executeCalls(
                ImmutableSingleCall.of(0L, 1L, LEADER_1),
                ImmutableSingleCall.of(3L, 36L, LEADER_1),
                ImmutableSingleCall.of(10L, 15L, LEADER_3),
                ImmutableSingleCall.of(21L, 28L, LEADER_3));
        assertExpectedDurationAndLeaders(Instant.ofEpochMilli(3L), Instant.ofEpochMilli(15L), LEADER_1, LEADER_3);
    }

    private void verifyEndpointRunsWithTiming(Runnable endpointCall) {
        endpointCall.run();
        verify(mockedClock, times(2)).instant();
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

    @Test
    public void statisticsCausesMetricRegistryToBeReset() {
        LockWatchStateUpdate.Snapshot secondUpdate =
                LockWatchStateUpdate.snapshot(UUID.randomUUID(), 1L, ImmutableSet.of(), ImmutableSet.of());
        LockWatchStateUpdate thirdUpdate = LockWatchStateUpdate.success(UUID.randomUUID(), 5L, ImmutableList.of());
        when(startTransactionsResponse.getLockWatchUpdate())
                .thenReturn(UPDATE)
                .thenReturn(secondUpdate)
                .thenReturn(secondUpdate)
                .thenReturn(thirdUpdate);
        timelockService.startTransactions(startTransactionsRequest);
        timelockService.startTransactions(startTransactionsRequest);

        TaggedMetricRegistry newMockedRegistry = mock(TaggedMetricRegistry.class);
        Timer newMockedTimer = mock(Timer.class);
        when(newMockedRegistry.timer(any())).thenReturn(newMockedTimer);
        timelockService.getStatisticsAndSetRegistryTo(newMockedRegistry);

        verify(mockedRegistry, atLeastOnce()).timer(any());
        verify(mockedTimer).update(anyLong(), any());
        verify(mockedTimer).getSnapshot();
        verify(mockedSnapshot).size();

        timelockService.startTransactions(startTransactionsRequest);
        timelockService.startTransactions(startTransactionsRequest);

        verify(newMockedRegistry, atLeastOnce()).timer(any());
        verify(newMockedTimer).update(anyLong(), any());
        verifyNoMoreInteractions(mockedRegistry);
        verifyNoMoreInteractions(mockedTimer);
    }

    private void assertExpectedDurationAndLeaders(Instant instant, Instant instant2, UUID oldLeader, UUID newLeader) {
        LeaderElectionDuration estimate =
                timelockService.calculateLastLeaderElectionDuration().get();
        assertThat(estimate.getDuration()).isEqualTo(Duration.between(instant, instant2));
        assertThat(estimate.getOldLeader()).isEqualTo(oldLeader);
        assertThat(estimate.getNewLeader()).isEqualTo(newLeader);
    }

    @Value.Immutable
    interface SingleCall {
        @Parameter
        long requestMillis();

        @Parameter
        long responseMillis();

        @Parameter
        UUID responseLeader();

        static SingleCall of(long requestMillis, long responseMillis, UUID responseLeader) {
            return ImmutableSingleCall.of(requestMillis, responseMillis, responseLeader);
        }
    }
}
