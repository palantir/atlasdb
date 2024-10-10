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
import com.palantir.atlasdb.timelock.api.NamedMinTimestampLeaseResponse;
import com.palantir.atlasdb.timelock.api.TimestampName;
import com.palantir.atlasdb.timelock.lock.AsyncLockService;
import com.palantir.atlasdb.timelock.lock.LockLog;
import com.palantir.atlasdb.timelock.lockwatches.BufferMetrics;
import com.palantir.atlasdb.timelock.lockwatches.RequestMetrics;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.ManagedTimestampService;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public final class AsyncTimelockServiceImplTest {
    private static final TimestampName TIMESTAMP_NAME = TimestampName.of("CommitImmutableTimestamp");

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
    public void acquireNamedMinTimestampLeaseReturnsLockTokenWithGivenRequestId() {
        UUID requestId = UUID.randomUUID();
        NamedMinTimestampLeaseResponse acquireResponse = acquireNamedMinTimestampLease(requestId, 10);
        assertThat(acquireResponse.getLock().getRequestId()).isEqualTo(requestId);
    }

    @Test
    public void acquireNamedMinTimestampLeaseReturnsMinLeased() {
        // The assumption is that the timestamp service used gives incremental timestamps starting
        // with 0 and with no gaps. Thus, a call to acquire is expected to fetch the number of
        // timestamps requested plus one.

        // Expected to lock with 1
        NamedMinTimestampLeaseResponse response1 = acquireNamedMinTimestampLease(10);
        assertThat(response1.getMinLeased()).isEqualTo(1);

        // Expected to lock with 12
        NamedMinTimestampLeaseResponse response2 = acquireNamedMinTimestampLease(5);
        assertThat(response2.getMinLeased()).isEqualTo(1);

        assertThat(lockService.unlock(response1.getLock())).isTrue();
        // Expected to lock with 18
        NamedMinTimestampLeaseResponse response3 = acquireNamedMinTimestampLease(7);
        assertThat(response3.getMinLeased()).isEqualTo(12);

        assertThat(lockService.unlock(response2.getLock())).isTrue();
        // Expected to lock with 26
        NamedMinTimestampLeaseResponse response4 = acquireNamedMinTimestampLease(8);
        assertThat(response4.getMinLeased()).isEqualTo(18);
    }

    @Test
    public void acquireNamedMinTimestampLeaseReturnsFreshTimestampsGreaterThanTheLockedLease() {
        NamedMinTimestampLeaseResponse response = acquireNamedMinTimestampLease(10);
        assertThat(response.getFreshTimestamps().getStart()).isGreaterThan(response.getMinLeased());
    }

    @ValueSource(ints = {1, 5, 10})
    @ParameterizedTest
    public void acquireNamedMinTimestampLeaseReturnsExactlyTheNumberOfFreshTimestampsRequested(int count) {
        NamedMinTimestampLeaseResponse response = acquireNamedMinTimestampLease(count);
        // This is a stronger guarantee than what we provide, but assuming the timestamp
        // service used returns increments of 1, the tests allows us to verify we pass
        // the right input to the timestamp service
        assertThat(response.getFreshTimestamps().getCount()).isEqualTo(count);
    }

    @Test
    public void acquireNamedMinTimestampLeaseLocksWithAFreshTimestamp() {
        timestampService.fastForwardTimestamp(10000L);
        NamedMinTimestampLeaseResponse response = acquireNamedMinTimestampLease(10);
        assertThat(response.getMinLeased()).isEqualTo(10001L);
    }

    @Test
    public void getNamedMinTimestampLeaseReturnsFreshTimestampWhenNoLeaseIsHeld() {
        timestampService.fastForwardTimestamp(10000L);
        assertThat(getMinLeasedNamedTimestamp()).isEqualTo(10001L);
    }

    @Test
    public void getNamedMinTimestampLeaseReturnsMinLeasedWhenLeaseIsHeld() {
        // The assumption is that the timestamp service used gives incremental timestamps starting
        // with 0 and with no gaps. Thus, a call to acquire is expected to fetch the number of
        // timestamps requested plus one.

        // For the math to make sense, we need to account for getMinLeasedNamedTimestamp grabbing
        // a fresh timestamp before the value is fetched.

        // Expected to lock with 1
        NamedMinTimestampLeaseResponse response1 = acquireNamedMinTimestampLease(10);
        assertThat(getMinLeasedNamedTimestamp()).isEqualTo(1);

        // Expected to lock with 13
        NamedMinTimestampLeaseResponse response2 = acquireNamedMinTimestampLease(5);
        assertThat(getMinLeasedNamedTimestamp()).isEqualTo(1);

        assertThat(lockService.unlock(response1.getLock())).isTrue();
        // Expected to lock with 20
        acquireNamedMinTimestampLease(7);
        assertThat(getMinLeasedNamedTimestamp()).isEqualTo(13);

        assertThat(lockService.unlock(response2.getLock())).isTrue();
        // Expected to lock with 29
        acquireNamedMinTimestampLease(8);
        assertThat(getMinLeasedNamedTimestamp()).isEqualTo(20);
    }

    private NamedMinTimestampLeaseResponse acquireNamedMinTimestampLease(int numFreshTimestamps) {
        return acquireNamedMinTimestampLease(UUID.randomUUID(), numFreshTimestamps);
    }

    private NamedMinTimestampLeaseResponse acquireNamedMinTimestampLease(UUID requestId, int numFreshTimestamps) {
        return unwrap(service.acquireNamedMinTimestampLease(TIMESTAMP_NAME, requestId, numFreshTimestamps));
    }

    private long getMinLeasedNamedTimestamp() {
        return unwrap(service.getMinLeasedNamedTimestamp(TIMESTAMP_NAME));
    }

    private <T> T unwrap(ListenableFuture<T> future) {
        executor.runUntilIdle();
        return AtlasFutures.getUnchecked(future);
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
}
