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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import com.palantir.common.time.NanoTime;
import com.palantir.lock.v2.ImmutablePartitionedTimestamps;
import com.palantir.lock.v2.PartitionedTimestamps;
import com.palantir.lock.v2.StartTransactionResponseV4;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.LeadershipId;
import com.palantir.lock.v2.Lease;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;

@RunWith(MockitoJUnitRunner.class)
public class CoalescingTransactionServiceTest {
    @Mock private LockLeaseService lockLeaseService;
    private CoalescingTransactionService transactionService;

    private static final int NUM_PARTITIONS = 16;
    private static final LockImmutableTimestampResponse IMMUTABLE_TS_RESPONSE =
            LockImmutableTimestampResponse.of(1L, LockToken.of(UUID.randomUUID()));

    private static final Lease LEASE = Lease.of(
            LeaderTime.of(LeadershipId.random(), NanoTime.createForTests(1L)),
            Duration.ofSeconds(1L));

    @Before
    public void before() {
        transactionService = CoalescingTransactionService.create(lockLeaseService);
    }

    @Test
    public void splitShouldYieldCorrectStartTransactionResponses_singleTransaction() {
        StartTransactionResponseV4 batchedResponse = getStartTransactionResponse(10, 1);

        assertThat(CoalescingTransactionService.split(batchedResponse))
                .hasSize(1)
                .allSatisfy(startTxnResponse -> assertDerivableFromBatchedResponse(startTxnResponse, batchedResponse));
    }

    @Test
    public void splitShouldYieldCorrectStartTransactionResponses_multipleTransactions() {
        StartTransactionResponseV4 batchedResponse = getStartTransactionResponse(10, 5);

        List<StartIdentifiedAtlasDbTransactionResponse> responses = CoalescingTransactionService.split(batchedResponse);

        assertThatStartTransactionResponsesAreUnique(responses);
        assertThat(CoalescingTransactionService.split(batchedResponse))
                .hasSize(5)
                .allSatisfy(startTxnResponse -> assertDerivableFromBatchedResponse(startTxnResponse, batchedResponse));
    }

    @Test
    public void shouldDeriveStartTransactionResponseFromBatchedResponse_singleTransaction() {
        StartTransactionResponseV4 startTransactionResponse = getStartTransactionResponse(12, 1);

        when(lockLeaseService.startTransactions(1)).thenReturn(startTransactionResponse);
        StartIdentifiedAtlasDbTransactionResponse response = transactionService.startIdentifiedAtlasDbTransaction();

        assertDerivableFromBatchedResponse(response, startTransactionResponse);
    }

    @Test
    public void shouldDeriveStartTransactionResponseFromBatchedResponse_multipleTransactions() throws Exception {
        ExecutorService executorService = Executors.newCachedThreadPool();

        BlockingBatchedResponse blockingBatchedResponse =
                new BlockingBatchedResponse(getStartTransactionResponse(12, 1));

        StartTransactionResponseV4 batchedStartTransactionResponse = getStartTransactionResponse(40, 2);

        when(lockLeaseService.startTransactions(anyInt()))
                .thenAnswer(blockingBatchedResponse)
                .thenReturn(getStartTransactionResponse(40, 2));

        CompletableFuture.runAsync(transactionService::startIdentifiedAtlasDbTransaction, executorService);

        blockingBatchedResponse.waitForFirstCallToBlock();

        CompletableFuture<StartIdentifiedAtlasDbTransactionResponse> response_1 =
                CompletableFuture.supplyAsync(transactionService::startIdentifiedAtlasDbTransaction, executorService);

        CompletableFuture<StartIdentifiedAtlasDbTransactionResponse> response_2 =
                CompletableFuture.supplyAsync(transactionService::startIdentifiedAtlasDbTransaction, executorService);

        blockingBatchedResponse.unblock();

        assertThatStartTransactionResponsesAreUnique(response_1.get(), response_2.get());
        assertDerivableFromBatchedResponse(response_1.get(), batchedStartTransactionResponse);
        assertDerivableFromBatchedResponse(response_2.get(), batchedStartTransactionResponse);

    }

    private void assertThatStartTransactionResponsesAreUnique(StartIdentifiedAtlasDbTransactionResponse... responses) {
        assertThatStartTransactionResponsesAreUnique(Arrays.asList(responses));
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

    private static final class BlockingBatchedResponse implements Answer<StartTransactionResponseV4> {
        private final CountDownLatch returnBatchedResponseLatch = new CountDownLatch(1);
        private final CountDownLatch blockingLatch = new CountDownLatch(1);
        private final StartTransactionResponseV4 batchedStartTransactionResponse;

        private BlockingBatchedResponse(StartTransactionResponseV4 batchedStartTransactionResponse) {
            this.batchedStartTransactionResponse = batchedStartTransactionResponse;
        }

        @Override
        public StartTransactionResponseV4 answer(InvocationOnMock invocation) throws Throwable {
            blockingLatch.countDown();
            returnBatchedResponseLatch.await();
            return batchedStartTransactionResponse;
        }

        void unblock() {
            returnBatchedResponseLatch.countDown();
        }

        void waitForFirstCallToBlock() throws InterruptedException {
            blockingLatch.await();
        }
    }
}