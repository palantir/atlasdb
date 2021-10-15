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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.correctness.TimestampCorrectnessMetrics;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsRequest;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsResponse;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsRequest;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
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
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public final class TimestampCorroboratingTimelockService2Test {
    private static final LockImmutableTimestampResponse LOCK_IMMUTABLE_TIMESTAMP_RESPONSE =
            LockImmutableTimestampResponse.of(1L, LockToken.of(UUID.randomUUID()));

    private static final String NAMESPACE_1 = "tom";
    private static final String NAMESPACE_2 = "nottom";
    public static final ConjureStartTransactionsRequest START_TRANSACTIONS_REQUEST =
            ConjureStartTransactionsRequest.builder()
                    .requestId(UUID.randomUUID())
                    .requestorId(UUID.randomUUID())
                    .numTransactions(1)
                    .build();

    private Runnable callback;
    private NamespacedConjureTimelockService rawTimelockService;
    private NamespacedConjureTimelockService timelockService;

    @Before
    public void setUp() {
        callback = mock(Runnable.class);
        rawTimelockService = mock(NamespacedConjureTimelockService.class);
        timelockService = new TimestampCorroboratingTimelockService2(callback, rawTimelockService);
    }

    @Test
    public void getFreshTimestampShouldFail() {
        when(rawTimelockService.getFreshTimestamps(any())).thenReturn(ConjureGetFreshTimestampsResponse.of(1L, 1L));
        assertThrowsOnSecondCall(() -> timelockService.getFreshTimestamps(ConjureGetFreshTimestampsRequest.of(1)));
        verify(callback).run();
    }

    @Test
    public void getFreshTimestampsShouldFail() {
        when(rawTimelockService.getFreshTimestamps(any())).thenReturn(ConjureGetFreshTimestampsResponse.of(1L, 2L));
        assertThrowsOnSecondCall(() -> timelockService.getFreshTimestamps(ConjureGetFreshTimestampsRequest.of(1)));
        verify(callback).run();
    }

    @Test
    public void startIdentifiedAtlasDbTransactionShouldFail() {
        when(rawTimelockService.startTransactions(START_TRANSACTIONS_REQUEST)).thenReturn(makeResponse(1L, 1));

        assertThrowsOnSecondCall(() -> timelockService.startTransactions(START_TRANSACTIONS_REQUEST));
        verify(callback).run();
    }

    @Test
    public void failsUnderConflictingMixedOperations() {
        ConjureStartTransactionsResponse startTransactionsResponse = makeResponse(1L, 1);
        when(rawTimelockService.startTransactions(START_TRANSACTIONS_REQUEST)).thenReturn(startTransactionsResponse);
        when(rawTimelockService.getFreshTimestamps(any())).thenReturn(ConjureGetFreshTimestampsResponse.of(1L, 2L));

        timelockService.startTransactions(START_TRANSACTIONS_REQUEST);
        assertThrowsClocksWentBackwardsException(
                () -> timelockService.getFreshTimestamps(ConjureGetFreshTimestampsRequest.of(2)));
        verify(callback).run();
    }

    @Test
    public void startIdentifiedAtlasDbTransactionBatchShouldFail() {
        ConjureStartTransactionsResponse responses = makeResponse(1L, 3);
        when(rawTimelockService.startTransactions(any())).thenReturn(responses);
        assertThrowsOnSecondCall(() -> timelockService.startTransactions(START_TRANSACTIONS_REQUEST));
        verify(callback).run();
    }

    @Test
    public void resilientUnderMultipleThreads() throws InterruptedException {
        BlockingTimestamp blockingTimestampReturning1 = new BlockingTimestamp(1);
        when(rawTimelockService.getFreshTimestamps(any()))
                .thenAnswer(blockingTimestampReturning1)
                .thenReturn(ConjureGetFreshTimestampsResponse.of(2L, 2L));

        Future<Void> blockingGetFreshTimestampCall = CompletableFuture.runAsync(
                () -> timelockService.getFreshTimestamps(ConjureGetFreshTimestampsRequest.of(1)));

        blockingTimestampReturning1.waitForFirstCallToBlock();

        assertThat(timelockService
                        .getFreshTimestamps(ConjureGetFreshTimestampsRequest.of(1))
                        .getInclusiveLower())
                .as("This should have updated the lower bound to 2")
                .isEqualTo(2L);

        // we want to now resume the blocked call, which will return timestamp of 1 and not throw
        blockingTimestampReturning1.countdown();
        assertThatCode(blockingGetFreshTimestampCall::get).doesNotThrowAnyException();
        verify(callback, never()).run();
    }

    @Test
    public void callbackInvokedMultipleTimesWithMultipleViolations() {
        when(rawTimelockService.getFreshTimestamps(any())).thenReturn(ConjureGetFreshTimestampsResponse.of(1L, 1L));

        timelockService.getFreshTimestamps(ConjureGetFreshTimestampsRequest.of(1));
        assertThrowsClocksWentBackwardsException(
                () -> timelockService.getFreshTimestamps(ConjureGetFreshTimestampsRequest.of(1)));
        assertThrowsClocksWentBackwardsException(
                () -> timelockService.getFreshTimestamps(ConjureGetFreshTimestampsRequest.of(1)));
        verify(callback, times(2)).run();
    }

    @Test
    public void metricsSuitablyIncremented() {
        when(rawTimelockService.getFreshTimestamp()).thenReturn(1L);
        TaggedMetricRegistry taggedMetricRegistry = new DefaultTaggedMetricRegistry();
        timelockService = TimestampCorroboratingTimelockService.create(
                Optional.of(NAMESPACE_1), taggedMetricRegistry, rawTimelockService);

        timelockService.getFreshTimestamp();
        assertThrowsClocksWentBackwardsException(timelockService::getFreshTimestamp);
        assertThrowsClocksWentBackwardsException(timelockService::getFreshTimestamp);

        assertThat(TimestampCorrectnessMetrics.of(taggedMetricRegistry)
                        .timestampsGoingBackwards(NAMESPACE_1)
                        .getCount())
                .isEqualTo(2);
        assertThat(TimestampCorrectnessMetrics.of(taggedMetricRegistry)
                        .timestampsGoingBackwards(NAMESPACE_2)
                        .getCount())
                .isEqualTo(0);
    }

    private ConjureStartTransactionsResponse makeResponse(long startTimestamp, int count) {
        return ConjureStartTransactionsResponse.builder()
                .immutableTimestamp(LOCK_IMMUTABLE_TIMESTAMP_RESPONSE)
                .lease(Lease.of(LeaderTime.of(LeadershipId.random(), NanoTime.now()), Duration.ZERO))
                .lockWatchUpdate(
                        LockWatchStateUpdate.snapshot(UUID.randomUUID(), -1L, ImmutableSet.of(), ImmutableSet.of()))
                .timestamps(ImmutablePartitionedTimestamps.builder()
                        .start(startTimestamp)
                        .count(count)
                        .interval(1)
                        .build())
                .build();
    }

    @Test
    public void metricsNotRegisteredIfNoViolationsDetected() {
        when(rawTimelockService.getFreshTimestamp()).thenReturn(1L);
        TaggedMetricRegistry taggedMetricRegistry = new DefaultTaggedMetricRegistry();
        timelockService = TimestampCorroboratingTimelockService.create(
                Optional.of(NAMESPACE_1), taggedMetricRegistry, rawTimelockService);

        timelockService.getFreshTimestamp();
        assertThat(taggedMetricRegistry.getMetrics()).isEmpty();
    }
    //
    //    private StartIdentifiedAtlasDbTransactionResponse makeResponse(long timestamp) {
    //        return StartIdentifiedAtlasDbTransactionResponse.of(
    //                LOCK_IMMUTABLE_TIMESTAMP_RESPONSE, TimestampAndPartition.of(timestamp, 0));
    //    }

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
            return ConjureGetFreshTimestampsResponse.of(timestampToReturn, timestampToReturn);
        }

        void countdown() {
            returnTimestampLatch.countDown();
        }

        void waitForFirstCallToBlock() throws InterruptedException {
            blockingLatch.await();
        }
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
}
