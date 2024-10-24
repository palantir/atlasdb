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
import com.palantir.atlasdb.common.api.timelock.TimestampLeaseName;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.timelock.api.TimestampLeaseResponse;
import com.palantir.atlasdb.timelock.api.TimestampLeaseResponses;
import com.palantir.atlasdb.timelock.lock.AsyncLockService;
import com.palantir.atlasdb.timelock.lock.LockLog;
import com.palantir.atlasdb.timelock.lockwatches.BufferMetrics;
import com.palantir.atlasdb.timelock.lockwatches.RequestMetrics;
import com.palantir.lock.v2.LockToken;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.ManagedTimestampService;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.jupiter.api.Test;

public final class AsyncTimelockServiceImplTest {
    private static final TimestampLeaseName TIMESTAMP_NAME_1 = TimestampLeaseName.of("ToyTimestamp_1");
    private static final TimestampLeaseName TIMESTAMP_NAME_2 = TimestampLeaseName.of("ToyTimestamp_2");

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
    public void unlockingWithRequestIdFromAcquireTimestampLeaseResponseUnlocksForAllRelevantTimestampNames() {
        TimestampedInvocation<Long> timestamp1MinLeased1 = getMinLeasedTimestampTimestamped(TIMESTAMP_NAME_1);
        assertThatTimestampIsStrictlyWithinInvocationInterval(timestamp1MinLeased1.result, timestamp1MinLeased1);
        TimestampedInvocation<Long> timestamp2MinLeased1 = getMinLeasedTimestampTimestamped(TIMESTAMP_NAME_2);
        assertThatTimestampIsStrictlyWithinInvocationInterval(timestamp2MinLeased1.result, timestamp2MinLeased1);

        TimestampedInvocation<TimestampLeaseResponses> responses =
                acquireTimestampLeaseTimestamped(TIMESTAMP_NAME_1, 10, TIMESTAMP_NAME_2, 20);
        assertThatTimestampsIsStrictlyWithinInvocationInterval(getMinLeasedTimestampsFrom(responses.result), responses);

        unlockForResponse(responses.result);
        assertThatTimestampIsStrictlyWithinInvocationInterval(timestamp1MinLeased1.result, timestamp1MinLeased1);
        assertThatTimestampIsStrictlyWithinInvocationInterval(timestamp2MinLeased1.result, timestamp2MinLeased1);
    }

    @Test
    public void acquireTimestampLeaseReturnsLeaseGuaranteeIdentifierWithGivenRequestId() {
        UUID requestId = UUID.randomUUID();
        TimestampLeaseResponses acquireResponse =
                acquireTimestampLease(requestId, TIMESTAMP_NAME_1, 10, TIMESTAMP_NAME_2, 20);
        assertThat(acquireResponse.getLeaseGuarantee().getIdentifier().get()).isEqualTo(requestId);
    }

    @Test
    public void acquireTimestampLeaseReturnsMinLeasedAllThroughout() {
        TimestampedInvocation<TimestampLeaseResponses> response1 =
                acquireTimestampLeaseTimestamped(TIMESTAMP_NAME_1, 15);
        assertThatTimestampsIsStrictlyWithinInvocationInterval(getMinLeasedTimestampsFrom(response1.result), response1);

        TimestampedInvocation<TimestampLeaseResponses> response2 =
                acquireTimestampLeaseTimestamped(TIMESTAMP_NAME_1, 5);
        assertThatTimestampsIsStrictlyWithinInvocationInterval(getMinLeasedTimestampsFrom(response2.result), response1);

        TimestampedInvocation<TimestampLeaseResponses> response3 =
                acquireTimestampLeaseTimestamped(TIMESTAMP_NAME_1, 7);
        assertThatTimestampsIsStrictlyWithinInvocationInterval(getMinLeasedTimestampsFrom(response3.result), response1);

        unlockForResponse(response1.result);
        unlockForResponse(response3.result);
        TimestampedInvocation<TimestampLeaseResponses> response4 =
                acquireTimestampLeaseTimestamped(TIMESTAMP_NAME_1, 19);
        assertThatTimestampsIsStrictlyWithinInvocationInterval(getMinLeasedTimestampsFrom(response4.result), response2);
    }

    @Test
    public void acquireTimestampLeaseReturnsFreshTimestampsGreaterThanReturnedMinLeased() {
        TimestampLeaseResponses response = acquireTimestampLease(TIMESTAMP_NAME_1, 199, TIMESTAMP_NAME_2, 87);

        TimestampLeaseResponse timestampName1Response =
                response.getTimestampLeaseResponses().get(TIMESTAMP_NAME_1);
        TimestampLeaseResponse timestampName2Response =
                response.getTimestampLeaseResponses().get(TIMESTAMP_NAME_2);

        assertThat(timestampName1Response.getFreshTimestamps().getStart())
                .isGreaterThan(timestampName1Response.getMinLeased());
        assertThat(timestampName2Response.getFreshTimestamps().getStart())
                .isGreaterThan(timestampName2Response.getMinLeased());
    }

    @Test
    public void acquireTimestampLeaseReturnedFreshTimestampsAreFreshTimestamps() {
        TimestampedInvocation<TimestampLeaseResponses> responses =
                acquireTimestampLeaseTimestamped(TIMESTAMP_NAME_1, 10);
        TimestampLeaseResponse response =
                responses.result.getTimestampLeaseResponses().get(TIMESTAMP_NAME_1);

        long firstTimestamp = response.getFreshTimestamps().getStart();
        long lastTimestamp = firstTimestamp + response.getFreshTimestamps().getCount() - 1;

        assertThatTimestampIsStrictlyWithinInvocationInterval(firstTimestamp, responses);
        assertThatTimestampIsStrictlyWithinInvocationInterval(lastTimestamp, responses);
    }

    @Test
    public void acquireTimestampLeaseLocksWithAFreshTimestamp() {
        TimestampedInvocation<TimestampLeaseResponses> responses =
                acquireTimestampLeaseTimestamped(TIMESTAMP_NAME_1, 10);
        TimestampLeaseResponse response =
                responses.result.getTimestampLeaseResponses().get(TIMESTAMP_NAME_1);

        assertThatTimestampIsStrictlyWithinInvocationInterval(response.getMinLeased(), responses);
    }

    @Test
    public void getMinLeasedTimestampReturnsFreshTimestampWhenNoLeaseIsHeld() {
        TimestampedInvocation<Long> response = getMinLeasedTimestampTimestamped(TIMESTAMP_NAME_1);
        assertThatTimestampIsStrictlyWithinInvocationInterval(response.result, response);
    }

    @Test
    public void getMinLeasedTimestampReturnsMinLeasedWhenLeaseIsHeld() {
        TimestampedInvocation<TimestampLeaseResponses> response1 =
                acquireTimestampLeaseTimestamped(TIMESTAMP_NAME_1, 10);
        assertThatTimestampIsStrictlyWithinInvocationInterval(getMinLeasedTimestamp(TIMESTAMP_NAME_1), response1);

        TimestampedInvocation<TimestampLeaseResponses> response2 =
                acquireTimestampLeaseTimestamped(TIMESTAMP_NAME_1, 5);
        assertThatTimestampIsStrictlyWithinInvocationInterval(getMinLeasedTimestamp(TIMESTAMP_NAME_1), response1);

        assertThat(unlockForResponse(response1.result)).isTrue();
        TimestampedInvocation<TimestampLeaseResponses> response3 =
                acquireTimestampLeaseTimestamped(TIMESTAMP_NAME_1, 7);
        assertThatTimestampIsStrictlyWithinInvocationInterval(getMinLeasedTimestamp(TIMESTAMP_NAME_1), response2);

        TimestampedInvocation<TimestampLeaseResponses> response4 =
                acquireTimestampLeaseTimestamped(TIMESTAMP_NAME_1, 12);
        assertThatTimestampIsStrictlyWithinInvocationInterval(getMinLeasedTimestamp(TIMESTAMP_NAME_1), response2);

        assertThat(unlockForResponse(response2.result)).isTrue();
        assertThatTimestampIsStrictlyWithinInvocationInterval(getMinLeasedTimestamp(TIMESTAMP_NAME_1), response3);

        assertThat(unlockForResponse(response3.result)).isTrue();
        assertThatTimestampIsStrictlyWithinInvocationInterval(getMinLeasedTimestamp(TIMESTAMP_NAME_1), response4);
    }

    private TimestampedInvocation<TimestampLeaseResponses> acquireTimestampLeaseTimestamped(
            TimestampLeaseName timestampName1, int requested1, TimestampLeaseName timestampName2, int requested2) {
        return new TimestampedInvocation<>(
                () -> acquireTimestampLease(timestampName1, requested1, timestampName2, requested2));
    }

    private TimestampedInvocation<TimestampLeaseResponses> acquireTimestampLeaseTimestamped(
            TimestampLeaseName timestampName, int requested) {
        return new TimestampedInvocation<>(() -> acquireTimestampLease(timestampName, requested));
    }

    private TimestampLeaseResponses acquireTimestampLease(
            TimestampLeaseName timestampLeaseName, int numFreshTimestamps) {
        return acquireTimestampLease(UUID.randomUUID(), timestampLeaseName, numFreshTimestamps);
    }

    private TimestampLeaseResponses acquireTimestampLease(
            UUID requestId, TimestampLeaseName timestampName, int numFreshTimestamps) {
        return unwrap(service.acquireTimestampLease(requestId, Map.of(timestampName, numFreshTimestamps)));
    }

    private TimestampLeaseResponses acquireTimestampLease(
            TimestampLeaseName timestampName1,
            int numFreshTimestamps1,
            TimestampLeaseName timestampLeaseName2,
            int numFreshTimestamps2) {
        return acquireTimestampLease(
                UUID.randomUUID(), timestampName1, numFreshTimestamps1, timestampLeaseName2, numFreshTimestamps2);
    }

    private TimestampLeaseResponses acquireTimestampLease(
            UUID requestId,
            TimestampLeaseName timestampName1,
            int numFreshTimestamps1,
            TimestampLeaseName timestampLeaseName2,
            int numFreshTimestamps2) {
        return unwrap(service.acquireTimestampLease(
                requestId, Map.of(timestampName1, numFreshTimestamps1, timestampLeaseName2, numFreshTimestamps2)));
    }

    private TimestampedInvocation<Long> getMinLeasedTimestampTimestamped(TimestampLeaseName name) {
        return new TimestampedInvocation<>(() -> getMinLeasedTimestamp(name));
    }

    private long getMinLeasedTimestamp(TimestampLeaseName timestampName) {
        return unwrap(service.getMinLeasedTimestamp(timestampName));
    }

    private boolean unlockForResponse(TimestampLeaseResponses response) {
        return lockService.unlock(createLockTokenFromResponse(response));
    }

    private <T> T unwrap(ListenableFuture<T> future) {
        executor.runUntilIdle();
        return AtlasFutures.getUnchecked(future);
    }

    private static List<Long> getMinLeasedTimestampsFrom(TimestampLeaseResponses response) {
        return response.getTimestampLeaseResponses().values().stream()
                .map(TimestampLeaseResponse::getMinLeased)
                .collect(Collectors.toList());
    }

    private static void assertThatTimestampsIsStrictlyWithinInvocationInterval(
            List<Long> timestamps, TimestampedInvocation<?> invocation) {
        assertThat(timestamps)
                .allSatisfy(timestamp -> assertThatTimestampIsStrictlyWithinInvocationInterval(timestamp, invocation));
    }

    private static void assertThatTimestampIsStrictlyWithinInvocationInterval(
            long timestamp, TimestampedInvocation<?> invocation) {
        assertThat(timestamp).isStrictlyBetween(invocation.timestampBefore, invocation.timestampAfter);
    }

    private static LockToken createLockTokenFromResponse(TimestampLeaseResponses response) {
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
