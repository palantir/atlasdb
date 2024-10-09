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

public final class AsyncTimelockServiceImplTest {
    private static final TimestampName TIMESTAMP_NAME = TimestampName.of("CommitImmutableTimestamp");

    private final DeterministicScheduler executor = new DeterministicScheduler();
    private final AsyncTimelockService service = createAsyncTimelockService(executor);

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

    private NamedMinTimestampLeaseResponse acquireNamedMinTimestampLease(UUID requestId, int numFreshTimestamps) {
        return unwrap(service.acquireNamedMinTimestampLease(TIMESTAMP_NAME, requestId, numFreshTimestamps));
    }

    @SuppressWarnings("UnusedMethod")
    private long getMinLeasedNamedTimestamp() {
        return unwrap(service.getMinLeasedNamedTimestamp(TIMESTAMP_NAME));
    }

    private <T> T unwrap(ListenableFuture<T> future) {
        executor.runUntilIdle();
        return AtlasFutures.getUnchecked(future);
    }

    private static AsyncTimelockService createAsyncTimelockService(ScheduledExecutorService executor) {
        LockLog lockLog = new LockLog(new MetricRegistry(), () -> 1000L);

        AsyncLockService asyncLockService = AsyncLockService.createDefault(
                lockLog, executor, executor, BufferMetrics.of(new DefaultTaggedMetricRegistry()));

        return new AsyncTimelockServiceImpl(
                asyncLockService,
                new InMemoryTimestampService(),
                lockLog,
                RequestMetrics.of(new DefaultTaggedMetricRegistry()));
    }
}
