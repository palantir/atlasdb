/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

import static java.util.stream.Collectors.toList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.IntStream;

import javax.annotation.Nullable;

import org.immutables.value.Value;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.palantir.atlasdb.autobatch.BatchElement;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.common.time.NanoTime;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.v2.ImmutablePartitionedTimestamps;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.LeadershipId;
import com.palantir.lock.v2.Lease;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.PartitionedTimestamps;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.watch.LockEvent;
import com.palantir.lock.watch.LockWatchEventCache;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.lock.watch.NoOpLockWatchEventCache;

@RunWith(MockitoJUnitRunner.class)
public class TransactionStarterTest {
    @Mock private LockLeaseService lockLeaseService;
    private LockWatchEventCache lockWatchEventCache = spy(NoOpLockWatchEventCache.INSTANCE);
    private TransactionStarter transactionStarter;

    private static final int NUM_PARTITIONS = 16;
    private static final LockImmutableTimestampResponse IMMUTABLE_TS_RESPONSE =
            LockImmutableTimestampResponse.of(1L, LockToken.of(UUID.randomUUID()));

    private static final Lease LEASE = Lease.of(
            LeaderTime.of(LeadershipId.random(), NanoTime.createForTests(1L)),
            Duration.ofSeconds(1L));
    private static final LockWatchStateUpdate UPDATE = LockWatchStateUpdate.success(
            UUID.randomUUID(),
            1,
            ImmutableList.of(LockEvent.builder(
                    ImmutableSet.of(StringLockDescriptor.of("lock")),
                    LockToken.of(UUID.randomUUID()))
                    .build(0)));

    @Before
    public void before() {
        transactionStarter = TransactionStarter.create(lockLeaseService, lockWatchEventCache);
    }

    @After
    public void after() {
        transactionStarter.close();
    }

    @Test
    public void shouldDeriveStartTransactionResponseFromBatchedResponse_singleTransaction() {
        ConjureStartTransactionsResponse startTransactionResponse = getStartTransactionResponse(12, 1);

        when(lockLeaseService.startTransactionsWithWatches(Optional.empty(), 1))
                .thenReturn(startTransactionResponse);
        StartIdentifiedAtlasDbTransactionResponse response = transactionStarter.startIdentifiedAtlasDbTransaction();

        assertDerivableFromBatchedResponse(response, startTransactionResponse);
    }

    @Test
    public void shouldDeriveStartTransactionResponseFromBatchedResponse_multipleTransactions() {
        ConjureStartTransactionsResponse batchResponse = getStartTransactionResponse(40, 3);
        when(lockLeaseService.startTransactionsWithWatches(Optional.empty(), 3))
                .thenReturn(batchResponse);

        List<StartIdentifiedAtlasDbTransactionResponse> responses = requestBatches(3);
        assertThat(responses)
                .satisfies(TransactionStarterTest::assertThatStartTransactionResponsesAreUnique)
                .hasSize(3)
                .allSatisfy(startTxnResponse -> assertDerivableFromBatchedResponse(startTxnResponse, batchResponse));
    }

    @Test
    public void shouldCallTimelockMultipleTimesUntilCollectsAllRequiredTimestampsAndProcessUpdates() {
        when(lockLeaseService.startTransactionsWithWatches(any(Optional.class), anyInt()))
                .thenReturn(getStartTransactionResponse(40, 2))
                .thenReturn(getStartTransactionResponse(100, 1));

        requestBatches(3);
        verify(lockLeaseService).startTransactionsWithWatches(Optional.empty(), 3);
        verify(lockLeaseService).startTransactionsWithWatches(Optional.empty(), 1);
        verify(lockWatchEventCache).processStartTransactionsUpdate(ImmutableSet.of(40L, 56L), UPDATE);
    }

    private List<StartIdentifiedAtlasDbTransactionResponse> requestBatches(int size) {
        List<BatchElement<Void, StartIdentifiedAtlasDbTransactionResponse>> elements = IntStream.range(0, size)
                .mapToObj(unused -> ImmutableTestBatchElement.builder()
                        .argument(null)
                        .result(SettableFuture.create())
                        .build())
                .collect(toList());
        TransactionStarter.consumer(lockLeaseService, lockWatchEventCache).accept(elements);
        return Futures.getUnchecked(Futures.allAsList(Lists.transform(elements, BatchElement::result)));
    }

    private static void assertThatStartTransactionResponsesAreUnique(
            List<? extends StartIdentifiedAtlasDbTransactionResponse> responses) {
        assertThat(responses)
                .as("Each response should have a different immutable ts lock token")
                .extracting(response -> response.immutableTimestamp().getLock().getRequestId())
                .doesNotHaveDuplicates();

        assertThat(responses)
                .as("Each response should have a different start timestamp")
                .extracting(response -> response.startTimestampAndPartition().timestamp())
                .doesNotHaveDuplicates();
    }

    private static void assertDerivableFromBatchedResponse(
            StartIdentifiedAtlasDbTransactionResponse startTransactionResponse,
            ConjureStartTransactionsResponse batchedStartTransactionResponse) {

        assertThat(startTransactionResponse.immutableTimestamp().getLock())
                .as("Should have a lock token share referencing to immutable ts lock token")
                .isInstanceOf(LockTokenShare.class)
                .extracting(t -> ((LockTokenShare) t).sharedLockToken())
                .isEqualTo(batchedStartTransactionResponse.getImmutableTimestamp().getLock());

        assertThat(startTransactionResponse.immutableTimestamp().getImmutableTimestamp())
                .as("Should have same immutable timestamp")
                .isEqualTo(batchedStartTransactionResponse.getImmutableTimestamp().getImmutableTimestamp());

        assertThat(startTransactionResponse.startTimestampAndPartition().partition())
                .as("Should have same partition value")
                .isEqualTo(batchedStartTransactionResponse.getTimestamps().partition());

        assertThat(batchedStartTransactionResponse.getTimestamps().stream())
                .as("Start timestamp should be contained by batched response")
                .contains(startTransactionResponse.startTimestampAndPartition().timestamp());
    }


    private static ConjureStartTransactionsResponse getStartTransactionResponse(
            long lowestStartTs, int count) {
        return ConjureStartTransactionsResponse.builder()
                .immutableTimestamp(IMMUTABLE_TS_RESPONSE)
                .timestamps(getPartitionedTimestamps(lowestStartTs, count))
                .lease(LEASE)
                .lockWatchUpdate(UPDATE)
                .build();
    }

    private static PartitionedTimestamps getPartitionedTimestamps(long startTs, int count) {
        return ImmutablePartitionedTimestamps.builder()
                .start(startTs)
                .count(count)
                .interval(NUM_PARTITIONS)
                .build();
    }

    @Value.Immutable
    interface TestBatchElement extends BatchElement<Void, StartIdentifiedAtlasDbTransactionResponse> {
        @Override
        @Nullable Void argument();
    }

}
