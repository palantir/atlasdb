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

package com.palantir.lock.client;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import com.palantir.util.Pair;

public class BatchingLockRefresherTest {
    private static final LockToken TOKEN_1 = LockToken.of(UUID.randomUUID());
    private static final LockToken TOKEN_2 = LockToken.of(UUID.randomUUID());
    private static final LockToken TOKEN_3 = LockToken.of(UUID.randomUUID());

    private static final Set<LockToken> TOKENS = ImmutableSet.of(TOKEN_1, TOKEN_2);
    private static final RuntimeException EXCEPTION = new RuntimeException("msg");

    private Set<LockToken> validTokens = ImmutableSet.of();
    private final TimelockService timelockService = mock(TimelockService.class);
    private final BatchingLockRefresher lockRefresher = BatchingLockRefresher.create(timelockService);


    @Test
    public void returnsRefreshedTokens() {
        when(timelockService.refreshLockLeases(any())).thenReturn(TOKENS);

        Set<LockToken> refreshedTokens = lockRefresher.refreshLockLeases(TOKENS);
        assertEquals(TOKENS, refreshedTokens);
    }

    @Test
    public void onlyReturnsValidTokens() {
        Set<LockToken> validTokens = ImmutableSet.of(TOKEN_1);
        when(timelockService.refreshLockLeases(any())).thenReturn(validTokens);

        Set<LockToken> refreshedTokens = lockRefresher.refreshLockLeases(TOKENS);
        assertEquals(validTokens, refreshedTokens);
    }

    @Test
    public void rethrowsException() {
        when(timelockService.refreshLockLeases(any())).thenThrow(EXCEPTION);

        assertThatThrownBy(() -> lockRefresher.refreshLockLeases(TOKENS)).isEqualTo(EXCEPTION);
    }

    @Test
    public void usingVerify() throws Exception {
        withValidTokens(TOKENS);
        verifyBatchedCallsWithResults(ImmutableList.of(
                new Pair<>(ImmutableSet.of(TOKEN_1), ImmutableSet.of(TOKEN_1)),
                new Pair<>(ImmutableSet.of(TOKEN_1, TOKEN_2), ImmutableSet.of(TOKEN_1, TOKEN_2)),
                new Pair<>(ImmutableSet.of(TOKEN_3), ImmutableSet.of()),
                new Pair<>(ImmutableSet.of(TOKEN_1, TOKEN_3), ImmutableSet.of(TOKEN_1))
        ));
    }

    private void verifyBatchedCallsWithResults(
            List<Pair<Set<LockToken>, Set<LockToken>>> refreshRequestsWithExpectedResults) throws Exception {
        RequestOrchestrator requestOrchestrator = new RequestOrchestrator();
        requestOrchestrator.sendBlockingRequest();

        List<Pair<Set<LockToken>, CompletableFuture<Set<LockToken>>>> result = refreshRequestsWithExpectedResults
                .stream()
                .map(p -> Pair.create(
                        p.lhSide,
                        CompletableFuture.supplyAsync(() -> lockRefresher.refreshLockLeases(p.lhSide))))
                .collect(Collectors.toList());

        requestOrchestrator.unblockRequest();

        for (int i = 0; i < result.size(); i++) {
            Set<LockToken> expectedResult = refreshRequestsWithExpectedResults.get(i).rhSide;
            Set<LockToken> actualResult = result.get(i).rhSide.get();
            assertEquals(expectedResult, actualResult);
        }

        verify(timelockService, times(2)).refreshLockLeases(anySet());
    }

    private class RequestOrchestrator {
        private final CountDownLatch firstBatchReached;
        private final CountDownLatch blockTimelockResponse;

        RequestOrchestrator() {
            this.firstBatchReached = new CountDownLatch(1);
            this.blockTimelockResponse = new CountDownLatch(1);
        }

        void sendBlockingRequest() throws InterruptedException {
            doAnswer(invocation -> {
                firstBatchReached.countDown();
                Set<LockToken> requestedTokens = invocation.getArgument(0);
                blockTimelockResponse.await();
                return requestedTokens.stream()
                        .filter(validTokens::contains)
                        .collect(Collectors.toSet());
            }).when(timelockService).refreshLockLeases(anySet());

            CompletableFuture.runAsync(() -> lockRefresher.refreshLockLeases(ImmutableSet.of()));
            firstBatchReached.await();
        }

        void unblockRequest() {
            blockTimelockResponse.countDown();
        }
    }

    private void withValidTokens(Set<LockToken> validTokens) {
        this.validTokens = validTokens;
    }
}