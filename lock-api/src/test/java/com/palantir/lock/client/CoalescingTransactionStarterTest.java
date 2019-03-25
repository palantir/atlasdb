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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import javax.annotation.Nullable;

import org.immutables.value.Value;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.palantir.atlasdb.autobatch.BatchElement;
import com.palantir.common.time.NanoTime;
import com.palantir.lock.v2.ImmutablePartitionedTimestamps;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.LeadershipId;
import com.palantir.lock.v2.Lease;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.PartitionedTimestamps;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.StartTransactionResponseV4;

@RunWith(MockitoJUnitRunner.class)
public class CoalescingTransactionStarterTest {
    @Mock private LockLeaseService lockLeaseService;
    private CoalescingTransactionStarter coalescingTransactionStarter;

    private static final int NUM_PARTITIONS = 16;
    private static final LockImmutableTimestampResponse IMMUTABLE_TS_RESPONSE =
            LockImmutableTimestampResponse.of(1L, LockToken.of(UUID.randomUUID()));

    private static final Lease LEASE = Lease.of(
            LeaderTime.of(LeadershipId.random(), NanoTime.createForTests(1L)),
            Duration.ofSeconds(1L));

    @Before
    public void before() {
        coalescingTransactionStarter = CoalescingTransactionStarter.create(lockLeaseService);
    }

    @Test
    public void splitShouldYieldCorrectStartTransactionResponses_singleTransaction() {
        StartTransactionResponseV4 batchedResponse = getStartTransactionResponse(10, 1);

        assertThat(CoalescingTransactionStarter.split(batchedResponse))
                .hasSize(1)
                .allSatisfy(startTxnResponse -> assertDerivableFromBatchedResponse(startTxnResponse, batchedResponse));
    }

    @Test
    public void splitShouldYieldCorrectStartTransactionResponses_multipleTransactions() {
        StartTransactionResponseV4 batchedResponse = getStartTransactionResponse(10, 5);

        List<StartIdentifiedAtlasDbTransactionResponse> responses =
                CoalescingTransactionStarter.split(batchedResponse);

        assertThatStartTransactionResponsesAreUnique(responses);
        assertThat(responses)
                .hasSize(5)
                .allSatisfy(startTxnResponse -> assertDerivableFromBatchedResponse(startTxnResponse, batchedResponse));
    }

    @Test
    public void shouldDeriveStartTransactionResponseFromBatchedResponse_singleTransaction() {
        StartTransactionResponseV4 startTransactionResponse = getStartTransactionResponse(12, 1);

        when(lockLeaseService.startTransactions(1)).thenReturn(startTransactionResponse);
        StartIdentifiedAtlasDbTransactionResponse response = coalescingTransactionStarter.startIdentifiedAtlasDbTransaction();

        assertDerivableFromBatchedResponse(response, startTransactionResponse);
    }

    @Test
    public void shouldDeriveStartTransactionResponseFromBatchedResponse_multipleTransactions() {
        StartTransactionResponseV4 batchResponse = getStartTransactionResponse(40, 3);
        when(lockLeaseService.startTransactions(3))
                .thenReturn(batchResponse);

        List<StartIdentifiedAtlasDbTransactionResponse> responses = requestBatches(3);
        assertThatStartTransactionResponsesAreUnique(responses);
        assertThat(responses)
                .hasSize(3)
                .allSatisfy(startTxnResponse -> assertDerivableFromBatchedResponse(startTxnResponse, batchResponse));
    }

    @Test
    public void shouldCallTimelockMultipleTimesUntilCollectsAllRequiredTimestamps() {
        when(lockLeaseService.startTransactions(anyInt()))
                .thenReturn(getStartTransactionResponse(40, 2))
                .thenReturn(getStartTransactionResponse(100, 1));

        requestBatches(3);
        verify(lockLeaseService).startTransactions(3);
        verify(lockLeaseService).startTransactions(1);

    }

    private List<StartIdentifiedAtlasDbTransactionResponse> requestBatches(int size) {
        List<BatchElement<Void, StartIdentifiedAtlasDbTransactionResponse>> elements = IntStream.range(0, size)
                .mapToObj(unused -> ImmutableTestBatchElement.builder()
                        .argument(null)
                        .result(SettableFuture.create())
                        .build())
                .collect(toList());
        CoalescingTransactionStarter.consumer(lockLeaseService).accept(elements);
        return Futures.getUnchecked(Futures.allAsList(Lists.transform(elements, BatchElement::result)));
    }

    private void assertThatStartTransactionResponsesAreUnique(
            List<StartIdentifiedAtlasDbTransactionResponse> responses) {
        assertThat(responses)
                .as("Each response should have a different immutable ts lock token")
                .extracting(response -> response.immutableTimestamp().getLock().getRequestId())
                .doesNotHaveDuplicates();

        assertThat(responses)
                .as("Each response should have a different start timestamp")
                .extracting(response -> response.startTimestampAndPartition().timestamp())
                .doesNotHaveDuplicates();
    }

    private void assertDerivableFromBatchedResponse(
            StartIdentifiedAtlasDbTransactionResponse startTransactionResponse,
            StartTransactionResponseV4 batchedStartTransactionResponse) {

        assertThat(startTransactionResponse.immutableTimestamp().getLock())
                .as("Should have a lock token share referencing to immutable ts lock token")
                .isInstanceOf(LockTokenShare.class)
                .extracting(t -> ((LockTokenShare) t).sharedLockToken())
                .isEqualTo(batchedStartTransactionResponse.immutableTimestamp().getLock());

        assertThat(startTransactionResponse.immutableTimestamp().getImmutableTimestamp())
                .as("Should have same immutable timestamp")
                .isEqualTo(batchedStartTransactionResponse.immutableTimestamp().getImmutableTimestamp());

        assertThat(startTransactionResponse.startTimestampAndPartition().partition())
                .as("Should have same partition value")
                .isEqualTo(batchedStartTransactionResponse.timestamps().partition());

        assertThat(batchedStartTransactionResponse.timestamps().stream())
                .as("Start timestamp should be contained by batched response")
                .contains(startTransactionResponse.startTimestampAndPartition().timestamp());
    }


    private static StartTransactionResponseV4 getStartTransactionResponse(
            long lowestStartTs, int count) {
        return StartTransactionResponseV4.of(
                IMMUTABLE_TS_RESPONSE,
                getPartitionedTimestamps(lowestStartTs, count),
                LEASE);
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
