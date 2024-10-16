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
package com.palantir.atlasdb.timelock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.timelock.api.TimestampLeaseName;
import com.palantir.atlasdb.timelock.api.TimestampLeaseResponse;
import com.palantir.atlasdb.timelock.lock.AsyncLockService;
import com.palantir.atlasdb.timelock.lock.LockLog;
import com.palantir.atlasdb.timelock.lockwatches.BufferMetrics;
import com.palantir.atlasdb.timelock.lockwatches.RequestMetrics;
import com.palantir.lock.v2.LockToken;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.ManagedTimestampService;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.jupiter.api.Test;

public final class AsyncTimelockServiceImplTest {
    private static final TimestampLeaseName TIMESTAMP_NAME = TimestampLeaseName.of("ToyTimestamp");

    private final DeterministicScheduler executor = new DeterministicScheduler();
    private final ManagedTimestampService timestampService = new InMemoryTimestampService();
    private final AsyncLockService lockService = createLockService(executor);
    private final AsyncTimelockService service = createAsyncTimelockService(timestampService, lockService);

    @Test
    public void delegatesInitializationCheck() {
        ManagedTimestampService mockMts = mock(ManagedTimestampService.class);
        AsyncTimelockServiceImpl timelockService = new AsyncTimelockServiceImpl(
                mock(AsyncLockService.class), mockMts, mock(LockLog.class), mock(RequestMetrics.class));
        when(mockMts.isInitialized()).thenReturn(false).thenReturn(true);

        assertThat(timelockService.isInitialized()).isFalse();
        assertThat(timelockService.isInitialized()).isTrue();
    }

    @Test
    public void acquireTimestampLeaseReturnsLeaseGuaranteeIdentifierWithGivenRequestId() {
        UUID requestId = UUID.randomUUID();
        TimestampLeaseResponse acquireResponse = acquireTimestampLease(requestId, 10);
        assertThat(acquireResponse.getLeaseGuarantee().getIdentifier().get()).isEqualTo(requestId);
    }

    @Test
    public void acquireTimestampLeaseReturnsMinLeasedAllThroughout() {
        TimestampedInvocation<TimestampLeaseResponse> response1 = acquireTimestampLeaseTimestamped(10);
        assertThatTimestampIsStrictlyWithinInvocationInterval(response1.result.getMinLeased(), response1);

        TimestampedInvocation<TimestampLeaseResponse> response2 = acquireTimestampLeaseTimestamped(5);
        assertThatTimestampIsStrictlyWithinInvocationInterval(response2.result.getMinLeased(), response1);

        unlockForResponse(response1.result);
        TimestampedInvocation<TimestampLeaseResponse> response3 = acquireTimestampLeaseTimestamped(7);
        assertThatTimestampIsStrictlyWithinInvocationInterval(response3.result.getMinLeased(), response2);

        unlockForResponse(response2.result);
        TimestampedInvocation<TimestampLeaseResponse> response4 = acquireTimestampLeaseTimestamped(19);
        assertThatTimestampIsStrictlyWithinInvocationInterval(response4.result.getMinLeased(), response3);
    }

    @Test
    public void acquireTimestampLeaseReturnsFreshTimestampsGreaterThanReturnedMinLeased() {
        TimestampLeaseResponse response = acquireTimestampLease(10);
        assertThat(response.getFreshTimestamps().getStart()).isGreaterThan(response.getMinLeased());
    }

    @Test
    public void acquireTimestampLeaseReturnedFreshTimestampsAreFreshTimestamps() {
        TimestampedInvocation<TimestampLeaseResponse> response = acquireTimestampLeaseTimestamped(10);
        long firstTimestamp = response.result.getFreshTimestamps().getStart();
        long lastTimestamp = firstTimestamp + response.result.getFreshTimestamps().getCount() - 1;
        assertThatTimestampIsStrictlyWithinInvocationInterval(firstTimestamp, response);
        assertThatTimestampIsStrictlyWithinInvocationInterval(lastTimestamp, response);
    }

    @Test
    public void acquireTimestampLeaseLocksWithAFreshTimestamp() {
        TimestampedInvocation<TimestampLeaseResponse> response = acquireTimestampLeaseTimestamped(10);
        assertThatTimestampIsStrictlyWithinInvocationInterval(response.result.getMinLeased(), response);
    }

    @Test
    public void getMinLeasedTimestampReturnsFreshTimestampWhenNoLeaseIsHeld() {
        TimestampedInvocation<Long> response = getMinLeasedTimestampTimestamped();
        assertThatTimestampIsStrictlyWithinInvocationInterval(response.result, response);
    }

    @Test
    public void getMinLeasedTimestampReturnsMinLeasedWhenLeaseIsHeld() {
        TimestampedInvocation<TimestampLeaseResponse> response1 = acquireTimestampLeaseTimestamped(10);
        assertThatTimestampIsStrictlyWithinInvocationInterval(getMinLeasedTimestamp(), response1);

        TimestampedInvocation<TimestampLeaseResponse> response2 = acquireTimestampLeaseTimestamped(5);
        assertThatTimestampIsStrictlyWithinInvocationInterval(getMinLeasedTimestamp(), response1);

        assertThat(unlockForResponse(response1.result)).isTrue();
        TimestampedInvocation<TimestampLeaseResponse> response3 = acquireTimestampLeaseTimestamped(7);
        assertThatTimestampIsStrictlyWithinInvocationInterval(getMinLeasedTimestamp(), response2);

        TimestampedInvocation<TimestampLeaseResponse> response4 = acquireTimestampLeaseTimestamped(12);
        assertThatTimestampIsStrictlyWithinInvocationInterval(getMinLeasedTimestamp(), response2);

        assertThat(unlockForResponse(response2.result)).isTrue();
        assertThatTimestampIsStrictlyWithinInvocationInterval(getMinLeasedTimestamp(), response3);

        assertThat(unlockForResponse(response3.result)).isTrue();
        assertThatTimestampIsStrictlyWithinInvocationInterval(getMinLeasedTimestamp(), response4);
    }

    private TimestampedInvocation<TimestampLeaseResponse> acquireTimestampLeaseTimestamped(int requested) {
        return new TimestampedInvocation<>(() -> acquireTimestampLease(requested));
    }

    private TimestampLeaseResponse acquireTimestampLease(int numFreshTimestamps) {
        return acquireTimestampLease(UUID.randomUUID(), numFreshTimestamps);
    }

    private TimestampLeaseResponse acquireTimestampLease(UUID requestId, int numFreshTimestamps) {
        return unwrap(service.acquireTimestampLease(TIMESTAMP_NAME, requestId, numFreshTimestamps));
    }

    private TimestampedInvocation<Long> getMinLeasedTimestampTimestamped() {
        return new TimestampedInvocation<>(this::getMinLeasedTimestamp);
    }

    private long getMinLeasedTimestamp() {
        return unwrap(service.getMinLeasedTimestamp(TIMESTAMP_NAME));
    }

    private boolean unlockForResponse(TimestampLeaseResponse response) {
        return lockService.unlock(createLockTokenFromResponse(response));
    }

    private <T> T unwrap(ListenableFuture<T> future) {
        executor.runUntilIdle();
        return AtlasFutures.getUnchecked(future);
    }

    private static void assertThatTimestampIsStrictlyWithinInvocationInterval(long timestamp, TimestampedInvocation<?> invocation) {
        assertThat(timestamp).isStrictlyBetween(invocation.timestampBefore, invocation.timestampAfter);
    }

    private static LockToken createLockTokenFromResponse(TimestampLeaseResponse response) {
        return LockToken.of(response.getLeaseGuarantee().getIdentifier().get());
    }

    private static AsyncLockService createLockService(ScheduledExecutorService executor) {
        return AsyncLockService.createDefault(
                new LockLog(new MetricRegistry(), () -> 100L),
                executor,
                executor,
                BufferMetrics.of(new DefaultTaggedMetricRegistry()));
    }

    private static AsyncTimelockService createAsyncTimelockService(
            ManagedTimestampService timestampService, AsyncLockService lockService) {
        return new AsyncTimelockServiceImpl(
                lockService,
                timestampService,
                new LockLog(new MetricRegistry(), () -> 1000L),
                RequestMetrics.of(new DefaultTaggedMetricRegistry()));
    }

    private final class TimestampedInvocation<T> {
        private final long timestampBefore;
        private final long timestampAfter;
        private final T result;

        private TimestampedInvocation(Supplier<T> invocation) {
            this.timestampBefore = timestampService.getFreshTimestamp();
            this.result = invocation.get();
            this.timestampAfter = timestampService.getFreshTimestamp();
        }
    }
}
