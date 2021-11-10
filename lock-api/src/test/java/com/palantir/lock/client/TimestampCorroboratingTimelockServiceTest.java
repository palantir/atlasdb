/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.correctness.TimestampCorrectnessMetrics;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsRequest;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsResponse;
import com.palantir.atlasdb.timelock.api.ConjureIdentifiedVersion;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsRequest;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsRequest;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsResponse;
import com.palantir.common.time.NanoTime;
import com.palantir.lock.v2.ImmutablePartitionedTimestamps;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.LeadershipId;
import com.palantir.lock.v2.Lease;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

@RunWith(MockitoJUnitRunner.class)
public final class TimestampCorroboratingTimelockServiceTest {
    private static final String NAMESPACE_1 = "sonic";
    private static final String NAMESPACE_2 = "shadow";
    private static final LockImmutableTimestampResponse LOCK_IMMUTABLE_TIMESTAMP_RESPONSE =
            LockImmutableTimestampResponse.of(1L, LockToken.of(UUID.randomUUID()));
    private static final LockWatchStateUpdate.Snapshot LOCK_WATCH_UPDATE =
            LockWatchStateUpdate.snapshot(UUID.randomUUID(), -1L, ImmutableSet.of(), ImmutableSet.of());

    @Mock
    private Runnable callback;

    @Mock
    private NamespacedConjureTimelockService rawTimelockService;

    @Mock
    private ConjureStartTransactionsRequest startTransactionsRequest;

    private TimestampCorroboratingTimelockService timelockService;

    @Before
    public void setUp() {
        timelockService = new TimestampCorroboratingTimelockService(callback, rawTimelockService);
    }

    @Test
    public void getFreshTimestampShouldFail() {
        when(rawTimelockService.getFreshTimestamps(any())).thenReturn(getFreshTimestampsResponse(1L, 1L));
        assertThrowsOnSecondCall(this::getFreshTimestamp);
        assertThat(timelockService.getTimestampBounds().boundFromFreshTimestamps().lowerBoundForNextRequest()).isEqualTo(1L);
        verify(callback).run();
    }

    @Test
    public void getFreshTimestampsShouldFail() {
        when(rawTimelockService.getFreshTimestamps(any())).thenReturn(getFreshTimestampsResponse(1L, 2L));
        assertThrowsOnSecondCall(() -> getFreshTimestamps(2));
        assertThat(timelockService.getTimestampBounds().boundFromFreshTimestamps().lowerBoundForNextRequest()).isEqualTo(2L);
        verify(callback).run();
    }

    @Test
    public void startTransactionsSingletonShouldFail() {
        when(rawTimelockService.startTransactions(startTransactionsRequest)).thenReturn(makeResponse(1L, 1));
        assertThrowsOnSecondCall(() -> timelockService.startTransactions(startTransactionsRequest));
        assertThat(timelockService.getTimestampBounds().boundFromTransactions()).isEqualTo(1L);
        verify(callback).run();
    }

    @Test
    public void startTransactionsUpdatesLowerBoundByItsUpperBound() {
        when(rawTimelockService.startTransactions(startTransactionsRequest)).thenReturn(makeResponse(1L, 20));
        timelockService.startTransactions(startTransactionsRequest);
        assertThat(timelockService.getTimestampBounds().boundFromTransactions()).isEqualTo(20L);
        verifyNoInteractions(callback);
    }

    @Test
    public void startTransactionsBoundIncreasesWithLargeInterval() {
        ConjureStartTransactionsResponse response = ConjureStartTransactionsResponse.builder()
                .immutableTimestamp(LOCK_IMMUTABLE_TIMESTAMP_RESPONSE)
                .lease(Lease.of(LeaderTime.of(LeadershipId.random(), NanoTime.now()), Duration.ZERO))
                .lockWatchUpdate(LOCK_WATCH_UPDATE)
                .timestamps(ImmutablePartitionedTimestamps.builder()
                        .start(5L)
                        .count(100)
                        .interval(12)
                        .build())
                .build();
        when(rawTimelockService.startTransactions(startTransactionsRequest)).thenReturn(response);
        timelockService.startTransactions(startTransactionsRequest);
        assertThat(timelockService.getTimestampBounds().boundFromTransactions()).isEqualTo(5 + (99 * 12));
        verifyNoInteractions(callback);
    }

    @Test
    public void startTransactionsThrowsIfSpanningBound() {
        when(rawTimelockService.getFreshTimestamps(any())).thenReturn(getFreshTimestampsResponse(10L, 20L));
        when(rawTimelockService.startTransactions(startTransactionsRequest)).thenReturn(makeResponse(15L, 30));
        getFreshTimestamps(11);
        assertThat(timelockService.getTimestampBounds().boundFromFreshTimestamps().lowerBoundForNextRequest()).isEqualTo(20L);
        assertThrowsClocksWentBackwardsException(() -> timelockService.startTransactions(startTransactionsRequest));
        verify(callback).run();
    }

    @Test
    public void getCommitTimestampsShouldFail() {
        when(rawTimelockService.getCommitTimestamps(any()))
                .thenReturn(GetCommitTimestampsResponse.of(1L, 3L, LOCK_WATCH_UPDATE));
        assertThrowsOnSecondCall(() -> timelockService.getCommitTimestamps(
                GetCommitTimestampsRequest.of(3, ConjureIdentifiedVersion.of(UUID.randomUUID(), 3L))));
        assertThat(timelockService.getTimestampBounds().boundFromCommitTimestamps().lowerBoundForNextRequest()).isEqualTo(3L);
        verify(callback).run();
    }

    @Test
    public void failsUnderConflictingMixedOperations() {
        ConjureStartTransactionsResponse startTransactionsResponse = makeResponse(1L, 1);
        when(rawTimelockService.startTransactions(startTransactionsRequest)).thenReturn(startTransactionsResponse);
        when(rawTimelockService.getFreshTimestamps(any())).thenReturn(getFreshTimestampsResponse(1L, 2L));
        timelockService.startTransactions(startTransactionsRequest);
        assertThrowsClocksWentBackwardsException(() -> getFreshTimestamps(2));
        assertThat(timelockService.getTimestampBounds().boundFromTransactions()).isEqualTo(1L);
        assertThat(timelockService.getTimestampBounds().boundFromFreshTimestamps().lowerBoundForNextRequest()).isEqualTo(Long.MIN_VALUE);
        verify(callback).run();
    }

    @Test
    public void startTransactionsBatchShouldFail() {
        ConjureStartTransactionsResponse responses = makeResponse(1L, 3);
        when(rawTimelockService.startTransactions(any())).thenReturn(responses);
        assertThrowsOnSecondCall(() -> timelockService.startTransactions(startTransactionsRequest));
        assertThat(timelockService.getTimestampBounds().boundFromTransactions()).isEqualTo(3L);
        verify(callback).run();
    }

    @Test
    public void resilientUnderMultipleThreads() throws InterruptedException {
        BlockingTimestamp blockingTimestampReturning = new BlockingTimestamp(1);
        when(rawTimelockService.getFreshTimestamps(any()))
                .thenAnswer(blockingTimestampReturning)
                .thenReturn(getFreshTimestampsResponse(2L, 2L));

        Future<Void> blockingGetFreshTimestampCall = CompletableFuture.runAsync(this::getFreshTimestamp);

        blockingTimestampReturning.waitForFirstCallToBlock();

        assertThat(getFreshTimestamp().getInclusiveLower())
                .as("This should have updated the lower bound to 2")
                .isEqualTo(2L);

        // we want to now resume the blocked call, which will return timestamp of 1 and not throw
        blockingTimestampReturning.countdown();
        assertThatCode(blockingGetFreshTimestampCall::get).doesNotThrowAnyException();
        assertThat(timelockService.getTimestampBounds().boundFromFreshTimestamps().lowerBoundForNextRequest()).isEqualTo(2L);
        verify(callback, never()).run();
    }

    @Test
    public void callbackInvokedMultipleTimesWithMultipleViolations() {
        when(rawTimelockService.getFreshTimestamps(any())).thenReturn(getFreshTimestampsResponse(1L, 1L));

        getFreshTimestamp();
        assertThrowsClocksWentBackwardsException(this::getFreshTimestamp);
        assertThrowsClocksWentBackwardsException(this::getFreshTimestamp);
        assertThat(timelockService.getTimestampBounds().boundFromFreshTimestamps().lowerBoundForNextRequest()).isEqualTo(1L);
        verify(callback, times(2)).run();
    }

    @Test
    public void metricsSuitablyIncremented() {
        when(rawTimelockService.getFreshTimestamps(any())).thenReturn(getFreshTimestampsResponse(1L, 1L));
        TaggedMetricRegistry taggedMetricRegistry = new DefaultTaggedMetricRegistry();
        timelockService = (TimestampCorroboratingTimelockService)
                TimestampCorroboratingTimelockService.create(NAMESPACE_1, taggedMetricRegistry, rawTimelockService);

        getFreshTimestamp();
        assertThrowsClocksWentBackwardsException(this::getFreshTimestamp);
        assertThrowsClocksWentBackwardsException(this::getFreshTimestamp);

        assertThat(TimestampCorrectnessMetrics.of(taggedMetricRegistry)
                        .timestampsGoingBackwards(NAMESPACE_1)
                        .getCount())
                .isEqualTo(2);
        assertThat(TimestampCorrectnessMetrics.of(taggedMetricRegistry)
                        .timestampsGoingBackwards(NAMESPACE_2)
                        .getCount())
                .isEqualTo(0);
    }

    @Test
    public void metricsNotRegisteredIfNoViolationsDetected() {
        when(rawTimelockService.getFreshTimestamps(any())).thenReturn(getFreshTimestampsResponse(1L, 1L));
        TaggedMetricRegistry taggedMetricRegistry = new DefaultTaggedMetricRegistry();
        timelockService = (TimestampCorroboratingTimelockService)
                TimestampCorroboratingTimelockService.create(NAMESPACE_1, taggedMetricRegistry, rawTimelockService);

        getFreshTimestamp();
        assertThat(taggedMetricRegistry.getMetrics()).isEmpty();
    }

    private ConjureGetFreshTimestampsResponse getFreshTimestamp() {
        return getFreshTimestamps(1);
    }

    private ConjureGetFreshTimestampsResponse getFreshTimestamps(int count) {
        return timelockService.getFreshTimestamps(ConjureGetFreshTimestampsRequest.of(count));
    }

    private void assertThrowsOnSecondCall(Runnable runnable) {
        runnable.run();
        assertThrowsClocksWentBackwardsException(runnable);
    }

    private void assertThrowsClocksWentBackwardsException(Runnable runnable) {
        assertThatThrownBy(runnable::run)
                .isInstanceOf(SafeRuntimeException.class)
                .hasMessageStartingWith("It appears that clocks went backwards!");
    }

    private static ConjureGetFreshTimestampsResponse getFreshTimestampsResponse(
            long startInclusive, long endInclusive) {
        return ConjureGetFreshTimestampsResponse.of(startInclusive, endInclusive);
    }

    private static ConjureStartTransactionsResponse makeResponse(long startTimestamp, int count) {
        return ConjureStartTransactionsResponse.builder()
                .immutableTimestamp(LOCK_IMMUTABLE_TIMESTAMP_RESPONSE)
                .lease(Lease.of(LeaderTime.of(LeadershipId.random(), NanoTime.now()), Duration.ZERO))
                .lockWatchUpdate(LOCK_WATCH_UPDATE)
                .timestamps(ImmutablePartitionedTimestamps.builder()
                        .start(startTimestamp)
                        .count(count)
                        .interval(1)
                        .build())
                .build();
    }

    private static final class BlockingTimestamp implements Answer<ConjureGetFreshTimestampsResponse> {
        private final CountDownLatch returnTimestampLatch = new CountDownLatch(1);
        private final CountDownLatch blockingLatch = new CountDownLatch(1);

        private final long timestampToReturn;

        private BlockingTimestamp(long timestampToReturn) {
            this.timestampToReturn = timestampToReturn;
        }

        @Override
        public ConjureGetFreshTimestampsResponse answer(InvocationOnMock invocation) throws Throwable {
            blockingLatch.countDown();
            returnTimestampLatch.await();
            return getFreshTimestampsResponse(timestampToReturn, timestampToReturn);
        }

        void countdown() {
            returnTimestampLatch.countDown();
        }

        void waitForFirstCallToBlock() throws InterruptedException {
            blockingLatch.await();
        }
    }
}
