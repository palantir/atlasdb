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

package com.palantir.atlasdb.factory.timelock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.ImmutableList;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.v2.TimestampAndPartition;
import com.palantir.timestamp.TimestampRange;

public class TimestampCorroboratingTimelockServiceTest {
    private static final LockImmutableTimestampResponse LOCK_IMMUTABLE_TIMESTAMP_RESPONSE =
            LockImmutableTimestampResponse.of(1L, LockToken.of(UUID.randomUUID()));

    private TimelockService rawTimelockService;
    private TimelockService timelockService;

    @Before
    public void setUp() {
        rawTimelockService = mock(TimelockService.class);
        timelockService = TimestampCorroboratingTimelockService.create(rawTimelockService);
    }

    @Test
    public void getFreshTimestampShouldFail() {
        when(rawTimelockService.getFreshTimestamp()).thenReturn(1L);

        assertThrowsOnSecondCall(timelockService::getFreshTimestamp);
    }

    @Test
    public void getFreshTimestampsShouldFail() {
        TimestampRange timestampRange = TimestampRange.createInclusiveRange(1, 2);
        when(rawTimelockService.getFreshTimestamps(anyInt())).thenReturn(timestampRange);

        assertThrowsOnSecondCall(() -> timelockService.getFreshTimestamps(1));
    }

    @Test
    public void startIdentifiedAtlasDbTransactionShouldFail() {
        StartIdentifiedAtlasDbTransactionResponse startIdentifiedAtlasDbTransactionResponse =
                makeResponse(1L);

        when(rawTimelockService.startIdentifiedAtlasDbTransactionBatch(1))
                .thenReturn(ImmutableList.of(startIdentifiedAtlasDbTransactionResponse));

        assertThrowsOnSecondCall(() -> timelockService.startIdentifiedAtlasDbTransactionBatch(1));
    }

    @Test
    public void startIdentifiedAtlasDbTransactionBatchShouldFail() {
        List<StartIdentifiedAtlasDbTransactionResponse> responses =
                ImmutableList.of(makeResponse(1L), makeResponse(2L), makeResponse(3L));

        when(rawTimelockService.startIdentifiedAtlasDbTransactionBatch(3)).thenReturn(responses);

        assertThrowsOnSecondCall(() -> timelockService.startIdentifiedAtlasDbTransactionBatch(3));
    }

    @Test
    public void resilientUnderMultipleThreads() throws InterruptedException {
        BlockingTimestamp blockingTimestampReturning1 = new BlockingTimestamp(1);
        when(rawTimelockService.getFreshTimestamp())
                .thenAnswer(blockingTimestampReturning1)
                .thenReturn(2L);

        Future<Void> blockingGetFreshTimestampCall =
                CompletableFuture.runAsync(timelockService::getFreshTimestamp);

        blockingTimestampReturning1.waitForFirstCallToBlock();

        assertThat(timelockService.getFreshTimestamp())
                .as("This should have updated the lower bound to 2")
                .isEqualTo(2L);

        // we want to now resume the blocked call, which will return timestamp of 1 and not throw
        blockingTimestampReturning1.countdown();
        assertThatCode(blockingGetFreshTimestampCall::get)
                .doesNotThrowAnyException();
    }

    private StartIdentifiedAtlasDbTransactionResponse makeResponse(long timestamp) {
        return StartIdentifiedAtlasDbTransactionResponse.of(LOCK_IMMUTABLE_TIMESTAMP_RESPONSE,
                TimestampAndPartition.of(timestamp, 0));
    }

    private static final class BlockingTimestamp implements Answer<Long> {
        private final CountDownLatch returnTimestampLatch = new CountDownLatch(1);
        private final CountDownLatch blockingLatch = new CountDownLatch(1);
        private final long timestampToReturn;

        private BlockingTimestamp(long timestampToReturn) {
            this.timestampToReturn = timestampToReturn;
        }

        @Override
        public Long answer(InvocationOnMock invocation) throws Throwable {
            blockingLatch.countDown();
            returnTimestampLatch.await();
            return timestampToReturn;
        }

        void countdown() {
            returnTimestampLatch.countDown();
        }

        void waitForFirstCallToBlock() throws InterruptedException {
            blockingLatch.await();
        }
    }

    private void assertThrowsOnSecondCall(Runnable runnable) {
        runnable.run();
        assertThatThrownBy(runnable::run)
                .isInstanceOf(AssertionError.class)
                .hasMessageStartingWith("Expected timestamp to be greater than");
    }
}
