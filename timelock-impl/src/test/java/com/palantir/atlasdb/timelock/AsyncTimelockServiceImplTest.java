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
import com.palantir.timestamp.TimestampService;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import java.util.Arrays;
import java.util.Comparator;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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

    // TODO(aalouane): enable when AsyncLockService implementation is merged
    @Disabled
    @Test
    public void acquireTimestampLeaseReturnsLeaseGuaranteeIdentifierWithGivenRequestId() {
        UUID requestId = UUID.randomUUID();
        TimestampLeaseResponse acquireResponse = acquireTimestampLease(requestId, 10);
        assertThat(acquireResponse.getLeaseGuarantee().getIdentifier().get()).isEqualTo(requestId);
    }

    // TODO(aalouane): enable when AsyncLockService implementation is merged
    @Disabled
    @Test
    public void acquireTimestampLeaseReturnsMinLeasedAllThroughout() {
        // The assumption is that the timestamp service used gives incremental timestamps starting
        // with 0 and with no gaps. Thus, a call to acquire is expected to fetch the number of
        // timestamps requested plus one.

        // Expected to lock with 1
        TimestampLeaseResponse response1 = acquireTimestampLease(10);
        assertThat(response1.getMinLeased()).isEqualTo(1);

        // Expected to lock with 12
        TimestampLeaseResponse response2 = acquireTimestampLease(5);
        assertThat(response2.getMinLeased()).isEqualTo(1);

        assertThat(lockService.unlock(createLockTokenFromResponse(response1))).isTrue();
        // Expected to lock with 18
        TimestampLeaseResponse response3 = acquireTimestampLease(7);
        assertThat(response3.getMinLeased()).isEqualTo(12);

        assertThat(lockService.unlock(createLockTokenFromResponse(response2))).isTrue();
        // Expected to lock with 26
        TimestampLeaseResponse response4 = acquireTimestampLease(8);
        assertThat(response4.getMinLeased()).isEqualTo(18);
    }

    // TODO(aalouane): enable when AsyncLockService implementation is merged
    @Disabled
    @Test
    public void acquireTimestampLeaseReturnsFreshTimestampsGreaterThanReturnedMinLeased() {
        TimestampLeaseResponse response = acquireTimestampLease(10);
        assertThat(response.getFreshTimestamps().getStart()).isGreaterThan(response.getMinLeased());
    }

    // TODO(aalouane): enable when AsyncLockService implementation is merged
    @Disabled
    @ValueSource(ints = {1, 5, 10})
    @ParameterizedTest
    public void acquireTimestampLeaseReturnsExactlyTheNumberOfFreshTimestampsRequested(int count) {
        TimestampLeaseResponse response = acquireTimestampLease(count);
        // This is a stronger guarantee than what we provide, but assuming the timestamp
        // service used returns increments of 1, the tests allows us to verify we pass
        // the right input to the timestamp service
        assertThat(response.getFreshTimestamps().getCount()).isEqualTo(count);
    }

    // TODO(aalouane): enable when AsyncLockService implementation is merged
    @Disabled
    @Test
    public void acquireTimestampLeaseLocksWithAFreshTimestamp() {
        timestampService.fastForwardTimestamp(10000L);
        TimestampLeaseResponse response = acquireTimestampLease(10);
        assertThat(response.getMinLeased()).isEqualTo(10001L);
    }

    // TODO(aalouane): enable when AsyncLockService implementation is merged
    @Disabled
    @Test
    public void getMinLeasedTimestampReturnsFreshTimestampWhenNoLeaseIsHeld() {
        withTimestampAssertScope(scope -> {
            long minLeasedTimestamp = getMinLeasedTimestamp();
            scope.assertWasFreshTimestamp(minLeasedTimestamp);
        });
    }

    // TODO(aalouane): enable when AsyncLockService implementation is merged
    @Disabled
    @Test
    public void getMinLeasedTimestampReturnsMinLeasedWhenLeaseIsHeld() {
        TimestampLeaseResponse response1 = withTimestampAssertScope(scope -> {
            TimestampLeaseResponse response = acquireTimestampLease(10);
            long minLeased = getMinLeasedTimestamp();
            scope.assertWasFreshTimestampLessThan(
                    minLeased, response.getFreshTimestamps().getStart());
            return response;
        });

        TimestampLeaseResponse response2 = acquireTimestampLease(5);
        assertThat(getMinLeasedTimestamp()).isEqualTo(getMinLeasedForResponses(response1, response2));

        assertThat(lockService.unlock(createLockTokenFromResponse(response1))).isTrue();
        acquireTimestampLease(7);
        assertThat(getMinLeasedTimestamp()).isEqualTo(getMinLeasedForResponses(response2));

        withTimestampAssertScope(scope -> {
            TimestampLeaseResponse response = acquireTimestampLease(8);
            long minLeased = getMinLeasedTimestamp();
            scope.assertWasFreshTimestampLessThan(
                    minLeased, response.getFreshTimestamps().getStart());
        });
    }

    private TimestampLeaseResponse acquireTimestampLease(int numFreshTimestamps) {
        return acquireTimestampLease(UUID.randomUUID(), numFreshTimestamps);
    }

    private TimestampLeaseResponse acquireTimestampLease(UUID requestId, int numFreshTimestamps) {
        return unwrap(service.acquireTimestampLease(TIMESTAMP_NAME, requestId, numFreshTimestamps));
    }

    private long getMinLeasedTimestamp() {
        return unwrap(service.getMinLeasedTimestamp(TIMESTAMP_NAME));
    }

    private <T> T unwrap(ListenableFuture<T> future) {
        executor.runUntilIdle();
        return AtlasFutures.getUnchecked(future);
    }

    private static long getMinLeasedForResponses(TimestampLeaseResponse... responses) {
        return Arrays.stream(responses)
                .map(TimestampLeaseResponse::getMinLeased)
                .min(Comparator.naturalOrder())
                .orElseThrow();
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

    private <T> T withTimestampAssertScope(Function<TimelockTimestampAssertScope, T> consumer) {
        return consumer.apply(new TimelockTimestampAssertScope(timestampService));
    }

    private void withTimestampAssertScope(Consumer<TimelockTimestampAssertScope> consumer) {
        consumer.accept(new TimelockTimestampAssertScope(timestampService));
    }

    private static final class TimelockTimestampAssertScope {
        private final TimestampService timestampService;
        private final long startTimestamp;

        private TimelockTimestampAssertScope(TimestampService timestampService) {
            this.timestampService = timestampService;
            this.startTimestamp = timestampService.getFreshTimestamp();
        }

        private void assertWasFreshTimestamp(long timestamp) {
            long laterTimestamp = timestampService.getFreshTimestamp();
            assertThat(timestamp).isStrictlyBetween(startTimestamp, laterTimestamp);
        }

        private void assertWasFreshTimestampLessThan(long timestamp, long upperBound) {
            assertWasFreshTimestamp(timestamp);
            assertThat(timestamp).isLessThan(upperBound);
        }
    }
}
