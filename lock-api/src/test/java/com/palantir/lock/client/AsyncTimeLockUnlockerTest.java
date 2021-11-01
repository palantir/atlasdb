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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.awaitility.Awaitility;
import org.awaitility.core.ThrowingRunnable;
import org.junit.Before;
import org.junit.Test;

public class AsyncTimeLockUnlockerTest {
    private static final int ONE_THOUSAND = 1000;

    private List<LockToken> tokenList;
    private List<LockToken> unlockedTokens;

    private TimelockService timelockService = mock(TimelockService.class);
    private AsyncTimeLockUnlocker unlocker = AsyncTimeLockUnlocker.create(timelockService);

    @Before
    public void setUp() {
        tokenList = createLockTokenList(ONE_THOUSAND);
        unlockedTokens = new ArrayList<>();
    }

    @Test(timeout = 2_000)
    public void enqueueDoesNotBlock() {
        doAnswer(_invocation -> {
                    Uninterruptibles.sleepUninterruptibly(Duration.ofSeconds(30));
                    return null;
                })
                .when(timelockService)
                .tryUnlock(any());

        unlocker.enqueue(ImmutableSet.copyOf(tokenList));
        // If enqueue blocked till unlock completed, this test would fail after 2 seconds
    }

    @Test
    public void enqueueingTokensEnqueuedSinglyAllEventuallyCaptured() {
        setupTokenCollectingTimeLock();

        tokenList.forEach(token -> unlocker.enqueue(ImmutableSet.of(token)));

        verifyTryUnlockAttemptedAtLeastOnce();
        assertAllTokensEventuallyUnlocked();
    }

    @Test
    public void enqueueingTokensEnqueuedInGroupsAllEventuallyCaptured() {
        setupTokenCollectingTimeLock();

        Lists.partition(tokenList, 7).forEach(tokens -> unlocker.enqueue(ImmutableSet.copyOf(tokens)));

        verifyTryUnlockAttemptedAtLeastOnce();
        assertAllTokensEventuallyUnlocked();
    }

    @SuppressWarnings("unchecked") // Mock invocation known to be correct
    @Test
    public void noParallelCallsMadeFromTimelockPointOfView() {
        AtomicBoolean timelockInUse = new AtomicBoolean(false);
        AtomicBoolean concurrentlyInvoked = new AtomicBoolean(false);

        doAnswer(invocation -> {
                    if (timelockInUse.get()) {
                        concurrentlyInvoked.set(true);
                    }
                    timelockInUse.set(true);
                    unlockedTokens.addAll((Set<LockToken>) invocation.getArguments()[0]);
                    timelockInUse.set(false);
                    return null;
                })
                .when(timelockService)
                .tryUnlock(any());

        tokenList.forEach(token -> unlocker.enqueue(ImmutableSet.of(token)));

        verifyTryUnlockAttemptedAtLeastOnce();
        assertAllTokensEventuallyUnlocked();
        assertThat(concurrentlyInvoked.get())
                .as("TimeLock was, at some point, called concurrently")
                .isFalse();
    }

    private static List<LockToken> createLockTokenList(int size) {
        return IntStream.range(0, size)
                .boxed()
                .map(_unused -> LockToken.of(UUID.randomUUID()))
                .collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked") // Mock invocation known to be correct
    private void setupTokenCollectingTimeLock() {
        doAnswer(invocation -> {
                    unlockedTokens.addAll((Set<LockToken>) invocation.getArguments()[0]);
                    return null;
                })
                .when(timelockService)
                .tryUnlock(any());
    }

    private void verifyTryUnlockAttemptedAtLeastOnce() {
        assertConditionEventuallyTrue(() -> {
            verify(timelockService, atLeastOnce()).tryUnlock(any());
            verifyNoMoreInteractions(timelockService);
        });
    }

    private void assertAllTokensEventuallyUnlocked() {
        assertConditionEventuallyTrue(() -> assertThat(unlockedTokens).hasSameElementsAs(tokenList));
    }

    private void assertConditionEventuallyTrue(ThrowingRunnable throwingRunnable) {
        Awaitility.await()
                .atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(throwingRunnable);
    }
}
