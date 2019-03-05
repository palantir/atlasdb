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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.Test;

import com.palantir.common.time.NanoTime;
import com.palantir.lock.v2.BatchedStartTransactionResponse;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.LeadershipId;
import com.palantir.lock.v2.Lease;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.TimestampAndPartition;
import com.palantir.lock.v2.TimestampRangeAndPartition;
import com.palantir.timestamp.TimestampRange;

public class CoalescingTransactionServiceTest {
    private LockLeaseService lockLeaseService = mock(LockLeaseService.class);
    private static final LockImmutableTimestampResponse IMMUTABLE_TS =
            LockImmutableTimestampResponse.of(1L, LockToken.of(UUID.randomUUID()));

    private static final Lease LEASE = Lease.of(
            LeaderTime.of(LeadershipId.random(), NanoTime.createForTests(1L)),
            Duration.ofSeconds(1L));

    @Test
    public void shouldReturnForSingleRequest() {
        when(lockLeaseService.batchedStartTransaction(1)).thenReturn(
                BatchedStartTransactionResponse.of(
                        IMMUTABLE_TS,
                        timestampRangeAndPartition(2, 18, 16),
                        LEASE
                ));

        CoalescingTransactionService transactionService = CoalescingTransactionService.create(lockLeaseService);
        StartIdentifiedAtlasDbTransactionResponse response = transactionService.startIdentifiedAtlasDbTransaction();

        LockImmutableTimestampResponse lockImmutableTimestampResponse = response.immutableTimestamp();
        TimestampAndPartition timestampAndPartition = response.startTimestampAndPartition();

        assertThat(lockImmutableTimestampResponse.getLock())
                .isInstanceOf(LockTokenShare.class)
                .extracting(t -> ((LockTokenShare) t).sharedLockToken())
                .isEqualTo(IMMUTABLE_TS.getLock());

        assertThat(timestampAndPartition)
                .extracting(TimestampAndPartition::partition).isEqualTo(16);

        assertThat(timestampAndPartition)
                .extracting(TimestampAndPartition::timestamp).isEqualTo(2L);
    }

    private static TimestampRangeAndPartition timestampRangeAndPartition(int lowerBound, int upperBound, int partition) {
        return TimestampRangeAndPartition.of(
                TimestampRange.createInclusiveRange(lowerBound, upperBound), partition);
    }

    @Test
    public void concurrentCase() throws Exception {
        CoalescingTransactionService transactionService = CoalescingTransactionService.create(lockLeaseService);
        ExecutorService executorService = Executors.newCachedThreadPool();
        Lock lock = new ReentrantLock();
        lock.lock();
        when(lockLeaseService.batchedStartTransaction(1)).thenAnswer(inv -> {
                lock.lock();
                return BatchedStartTransactionResponse.of(
                        IMMUTABLE_TS,
                        timestampRangeAndPartition(2, 18, 16),
                        LEASE
                );
        });

        when(lockLeaseService.batchedStartTransaction(3)).thenAnswer(inv -> {
            return BatchedStartTransactionResponse.of(
                    IMMUTABLE_TS,
                    timestampRangeAndPartition(2, 34, 16),
                    LEASE
            );
        });

        CompletableFuture<StartIdentifiedAtlasDbTransactionResponse> response =
                CompletableFuture.supplyAsync(transactionService::startIdentifiedAtlasDbTransaction, executorService);

        CompletableFuture<StartIdentifiedAtlasDbTransactionResponse> response_1 =
                CompletableFuture.supplyAsync(transactionService::startIdentifiedAtlasDbTransaction, executorService);

        CompletableFuture<StartIdentifiedAtlasDbTransactionResponse> response_2 =
                CompletableFuture.supplyAsync(transactionService::startIdentifiedAtlasDbTransaction, executorService);

        lock.unlock();

        System.out.println(response_2.get(10, TimeUnit.SECONDS));
        System.out.println(response_1.get(10, TimeUnit.SECONDS));
        System.out.println(response.get(10, TimeUnit.SECONDS));
    }

}