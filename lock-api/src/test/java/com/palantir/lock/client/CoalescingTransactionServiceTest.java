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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import com.palantir.common.time.NanoTime;
import com.palantir.lock.v2.BatchedStartTransactionResponse;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.LeadershipId;
import com.palantir.lock.v2.Lease;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.TimestampRangeAndPartition;
import com.palantir.timestamp.TimestampRange;

@RunWith(MockitoJUnitRunner.class)
public class CoalescingTransactionServiceTest {
    @Mock private LockLeaseService lockLeaseService;
    private CoalescingTransactionService transactionService;

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
    public void shouldDeriveStartTransactionResponseFromBatchedResponse_singleTransaction() {
        BatchedStartTransactionResponse batchedStartTransactionResponse =
                getBatchedStartTransactionResponse(12, 1);

        when(lockLeaseService.batchedStartTransaction(1)).thenReturn(batchedStartTransactionResponse);
        StartIdentifiedAtlasDbTransactionResponse response = transactionService.startIdentifiedAtlasDbTransaction();

        assertStartTransactionResponseIsDerivableFromBatchedResponse(response, batchedStartTransactionResponse);
    }

    @Test
    public void shouldDeriveStartTransactionResponseFromBatchedResponse_multipleTransactions() throws Exception {
        ExecutorService executorService = Executors.newCachedThreadPool();

        BlockingBatchedResponse blockingBatchedResponse =
                new BlockingBatchedResponse(getBatchedStartTransactionResponse(12, 1));

        BatchedStartTransactionResponse batchedStartTransactionResponse = getBatchedStartTransactionResponse(40, 2);

        when(lockLeaseService.batchedStartTransaction(anyInt()))
                .thenAnswer(blockingBatchedResponse)
                .thenReturn(getBatchedStartTransactionResponse(40, 2));

        CompletableFuture.runAsync(transactionService::startIdentifiedAtlasDbTransaction, executorService);

        blockingBatchedResponse.waitForFirstCallToBlock();

        CompletableFuture<StartIdentifiedAtlasDbTransactionResponse> response_1 =
                CompletableFuture.supplyAsync(transactionService::startIdentifiedAtlasDbTransaction, executorService);

        CompletableFuture<StartIdentifiedAtlasDbTransactionResponse> response_2 =
                CompletableFuture.supplyAsync(transactionService::startIdentifiedAtlasDbTransaction, executorService);

        blockingBatchedResponse.unblock();

        assertThatStartTransactionResponsesAreUnique(response_1.get(), response_2.get());
        assertStartTransactionResponseIsDerivableFromBatchedResponse(response_1.get(), batchedStartTransactionResponse);
        assertStartTransactionResponseIsDerivableFromBatchedResponse(response_2.get(), batchedStartTransactionResponse);

    }

    private void assertThatStartTransactionResponsesAreUnique(StartIdentifiedAtlasDbTransactionResponse... responses) {
        assertThat(responses)
                .as("Each response should have a different immutable ts lock token")
                .extracting(response -> response.immutableTimestamp().getLock().getRequestId())
                .doesNotHaveDuplicates();

        assertThat(responses)
                .as("Each response should have a different start timestamp")
                .extracting(response -> response.startTimestampAndPartition().timestamp())
                .doesNotHaveDuplicates();
    }

    private void assertStartTransactionResponseIsDerivableFromBatchedResponse(
            StartIdentifiedAtlasDbTransactionResponse startTransactionResponse,
            BatchedStartTransactionResponse batchedStartTransactionResponse) {

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
                .isEqualTo(batchedStartTransactionResponse.timestampRange().partition());

        assertThat(batchedStartTransactionResponse.timestampRange().getStartTimestamps())
                .as("Start timestamp should be contained by batched response")
                .contains(startTransactionResponse.startTimestampAndPartition().timestamp());
    }

    @Test
    public void splitShouldYieldCorrectNumberOfStartTransactionResponses_singleTransaction() {
        BatchedStartTransactionResponse batchedResponse = getBatchedStartTransactionResponse(10, 1);

        assertThat(CoalescingTransactionService.split(batchedResponse))
                .hasSize(1);
    }

    @Test
    public void splitShouldYieldCorrectNumberOfStartTransactionResponses_multipleTransactions() {
        BatchedStartTransactionResponse batchedResponse = getBatchedStartTransactionResponse(10, 5);

        assertThat(CoalescingTransactionService.split(batchedResponse))
                .hasSize(5);
    }

    @Test
    public void splitResultShouldHaveCorrectLockTokenShare_singleTransaction() {
        BatchedStartTransactionResponse batchedResponse = getBatchedStartTransactionResponse(10, 1);

        LockToken immutableTsLockToken =
                CoalescingTransactionService.split(batchedResponse).get(0).immutableTimestamp().getLock();

        assertThat(immutableTsLockToken)
                .isInstanceOf(LockTokenShare.class)
                .extracting(t -> ((LockTokenShare) t).sharedLockToken())
                .isEqualTo(IMMUTABLE_TS_RESPONSE.getLock());
    }

    @Test
    public void splitResultShouldHaveCorrectLockTokenShare_multipleTransactions() {
        BatchedStartTransactionResponse batchedResponse = getBatchedStartTransactionResponse(10, 3);

        List<LockToken> immutableTsLockTokens = CoalescingTransactionService.split(batchedResponse).stream()
                .map(startTransactionResponse -> startTransactionResponse.immutableTimestamp().getLock())
                .collect(Collectors.toList());

        assertThat(immutableTsLockTokens)
                .doesNotHaveDuplicates()
                .allMatch(t -> ((LockTokenShare) t).sharedLockToken().equals(IMMUTABLE_TS_RESPONSE.getLock()));
    }

    @Test
    public void splitResultShouldHaveCorrectImmutableTs_singleTransaction() {
        BatchedStartTransactionResponse batchedResponse = getBatchedStartTransactionResponse(10, 1);

        long immutableTs = CoalescingTransactionService.split(batchedResponse).get(0)
                .immutableTimestamp().getImmutableTimestamp();

        assertThat(immutableTs)
                .isEqualTo(IMMUTABLE_TS_RESPONSE.getImmutableTimestamp());
    }

    @Test
    public void splitResultShouldHaveCorrectImmutableTs_multipleTransactions() {
        BatchedStartTransactionResponse batchedResponse = getBatchedStartTransactionResponse(10, 3);

        List<Long> immutableTimestamps = CoalescingTransactionService.split(batchedResponse).stream()
                .map(startTransactionResponse -> startTransactionResponse.immutableTimestamp().getImmutableTimestamp())
                .collect(Collectors.toList());


        assertThat(immutableTimestamps)
                .allMatch(immutableTimestamp -> immutableTimestamp == IMMUTABLE_TS_RESPONSE.getImmutableTimestamp());
    }

    private static BatchedStartTransactionResponse getBatchedStartTransactionResponse(
            long lowestStartTs, long batchSize) {
        return BatchedStartTransactionResponse.of(
                IMMUTABLE_TS_RESPONSE,
                timestampRangeAndPartition(
                        lowestStartTs,
                        lowestStartTs + SharedConstants.TRANSACTION_NUM_PARTITIONS * batchSize - 1,
                        (int) lowestStartTs % SharedConstants.TRANSACTION_NUM_PARTITIONS),
                LEASE);
    }

    private static TimestampRangeAndPartition timestampRangeAndPartition(
            long lowerBound, long upperBound, int partition) {
        return TimestampRangeAndPartition.of(
                TimestampRange.createInclusiveRange(lowerBound, upperBound), partition);
    }

    private static final class BlockingBatchedResponse implements Answer<BatchedStartTransactionResponse> {
        private final CountDownLatch returnBatchedResponseLatch = new CountDownLatch(1);
        private final CountDownLatch blockingLatch = new CountDownLatch(1);
        private final BatchedStartTransactionResponse batchedStartTransactionResponse;

        private BlockingBatchedResponse(BatchedStartTransactionResponse batchedStartTransactionResponse) {
            this.batchedStartTransactionResponse = batchedStartTransactionResponse;
        }

        @Override
        public BatchedStartTransactionResponse answer(InvocationOnMock invocation) throws Throwable {
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