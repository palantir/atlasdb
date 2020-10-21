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
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;

public class LeaderElectionReportingTimelockServiceTest {
    private static final UUID LEADER_1 = UUID.randomUUID();
    private static final LockWatchStateUpdate UPDATE = LockWatchStateUpdate.success(LEADER_1, -1L, ImmutableList.of());

    private ConjureStartTransactionsRequest startTransactionsRequest = mock(ConjureStartTransactionsRequest.class);
    private ConjureStartTransactionsResponse startTransactionsResponse = mock(ConjureStartTransactionsResponse.class);
    private GetCommitTimestampsRequest commitTimestampsRequest = mock(GetCommitTimestampsRequest.class);
    private GetCommitTimestampsResponse commitTimestampsResponse = mock(GetCommitTimestampsResponse.class);
    private NamespacedConjureTimelockService mockedDelegate = mock(NamespacedConjureTimelockService.class);
    private TaggedMetricRegistry mockedRegistry = mock(TaggedMetricRegistry.class);
    private Timer mockedTimer = mock(Timer.class);

    private LeaderElectionReportingTimelockService timelockService;

    @Before
    public void before() {
        timelockService = new LeaderElectionReportingTimelockService(mockedDelegate, mockedRegistry);
        when(mockedDelegate.startTransactions(any())).thenReturn(startTransactionsResponse);
        when(mockedDelegate.getCommitTimestamps(any())).thenReturn(commitTimestampsResponse);
        when(mockedRegistry.timer(any())).thenReturn(mockedTimer);

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
        when(commitTimestampsResponse.getLockWatchUpdate())
                .thenReturn(LockWatchStateUpdate.success(UUID.randomUUID(), 5L, ImmutableList.of()));
        timelockService.startTransactions(startTransactionsRequest);
        Instant conservativeFirstLeaderUpperBound = Instant.now();
        timelockService.startTransactions(startTransactionsRequest);
        Instant conservativeSecondLeaderLowerBound = Instant.now();
        timelockService.getCommitTimestamps(commitTimestampsRequest);
        Optional<Duration> estimatedDuration = timelockService.calculateLastLeaderElectionDuration();
        assertThat(estimatedDuration).isPresent();
        assertThat(estimatedDuration.get())
                .isGreaterThan(Duration.between(conservativeFirstLeaderUpperBound, conservativeSecondLeaderLowerBound));
    }

    @Test
    public void detectLeaderElectionWithTwoLongTermLeaders() {
        when(commitTimestampsResponse.getLockWatchUpdate())
                .thenReturn(LockWatchStateUpdate.success(UUID.randomUUID(), 5L, ImmutableList.of()));
        timelockService.startTransactions(startTransactionsRequest);
        Instant conservativeFirstLeaderUpperBound = Instant.now();
        timelockService.startTransactions(startTransactionsRequest);
        Instant conservativeSecondLeaderLowerBound = Instant.now();
        timelockService.getCommitTimestamps(commitTimestampsRequest);
        timelockService.getCommitTimestamps(commitTimestampsRequest);
        Optional<Duration> estimatedDuration = timelockService.calculateLastLeaderElectionDuration();
        assertThat(estimatedDuration).isPresent();
        assertThat(estimatedDuration.get())
                .isGreaterThan(Duration.between(conservativeFirstLeaderUpperBound, conservativeSecondLeaderLowerBound));
    }

    @Test
    public void detectFreshLeaderElectionWithTwoLongTurnLeaders() {
        UUID secondLeader = UUID.randomUUID();
        UUID thirdLeader = UUID.randomUUID();
        when(commitTimestampsResponse.getLockWatchUpdate())
                .thenReturn(LockWatchStateUpdate.success(secondLeader, 5L, ImmutableList.of()))
                .thenReturn(LockWatchStateUpdate.success(secondLeader, 5L, ImmutableList.of()))
                .thenReturn(LockWatchStateUpdate.success(thirdLeader, 5L, ImmutableList.of()));

        timelockService.startTransactions(startTransactionsRequest);
        timelockService.startTransactions(startTransactionsRequest);
        timelockService.getCommitTimestamps(commitTimestampsRequest);
        Instant conservativeFirstLeaderUpperBound = Instant.now();
        timelockService.getCommitTimestamps(commitTimestampsRequest);
        Duration firstElectionDuration =
                timelockService.calculateLastLeaderElectionDuration().get();
        Instant conservativeSecondLeaderLowerBound = Instant.now();
        timelockService.getCommitTimestamps(commitTimestampsRequest);
        Optional<Duration> estimatedDuration = timelockService.calculateLastLeaderElectionDuration();
        assertThat(estimatedDuration).isPresent();
        assertThat(estimatedDuration.get()).isNotEqualTo(firstElectionDuration);
        assertThat(estimatedDuration.get())
                .isGreaterThan(Duration.between(conservativeFirstLeaderUpperBound, conservativeSecondLeaderLowerBound));
    }
}
