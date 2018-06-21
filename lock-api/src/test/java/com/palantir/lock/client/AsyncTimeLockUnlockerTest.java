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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;

public class AsyncTimeLockUnlockerTest {
    private static final int ONE_THOUSAND = 1000;

    private List<LockToken> tokenList;
    private List<LockToken> unlockedTokens;

    private TimelockService timelockService = mock(TimelockService.class);
    private AsyncTimeLockUnlocker unlocker
            = new AsyncTimeLockUnlocker(timelockService, PTExecutors.newSingleThreadScheduledExecutor());

    @Before
    public void setUp() {
        tokenList = createLockTokenList(ONE_THOUSAND);
        unlockedTokens = Lists.newArrayList();
    }

    @Test(timeout = 2_000)
    public void enqueueDoesNotBlock() {
        doAnswer(invocation -> {
            Uninterruptibles.sleepUninterruptibly(30, TimeUnit.SECONDS);
            return null;
        }).when(timelockService).tryUnlock(any());

        unlocker.enqueue(ImmutableSet.copyOf(tokenList));
        // If enqueue blocked till unlock completed, this test would fail after 2 seconds
    }

    @Test
    public void enqueueingTokensEnqueuedSinglyAllEventuallyCaptured() {
        setupTokenCollectingTimeLock();

        tokenList.forEach(token -> unlocker.enqueue(ImmutableSet.of(token)));
        Uninterruptibles.sleepUninterruptibly(4, TimeUnit.SECONDS);

        verifyTryUnlockAttemptedAtLeastOnce();
        assertThat(unlockedTokens).hasSameElementsAs(tokenList);
        assertAllTokensEventuallyUnlocked();
    }

    @Test
    public void enqueueingTokensEnqueuedInGroupsAllEventuallyCaptured() {
        setupTokenCollectingTimeLock();

        Iterables.partition(tokenList, 7).forEach(tokens -> unlocker.enqueue(ImmutableSet.copyOf(tokens)));

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
            unlockedTokens.addAll(((Set<LockToken>) invocation.getArguments()[0]));
            timelockInUse.set(false);
            return null;
        }).when(timelockService).tryUnlock(any());

        tokenList.forEach(token -> unlocker.enqueue(ImmutableSet.of(token)));

        verifyTryUnlockAttemptedAtLeastOnce();
        assertAllTokensEventuallyUnlocked();
        assertThat(concurrentlyInvoked.get()).as("TimeLock was, at some point, called concurrently").isFalse();
    }

    private static List<LockToken> createLockTokenList(int size) {
        return IntStream.range(0, size)
                .boxed()
                .map(unused -> LockToken.of(UUID.randomUUID()))
                .collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked") // Mock invocation known to be correct
    private void setupTokenCollectingTimeLock() {
        doAnswer(invocation -> {
            unlockedTokens.addAll(((Set<LockToken>) invocation.getArguments()[0]));
            return null;
        }).when(timelockService).tryUnlock(any());
    }

    private void verifyTryUnlockAttemptedAtLeastOnce() {
        verify(timelockService, atLeastOnce()).tryUnlock(any());
        verifyNoMoreInteractions(timelockService);
    }

    private void assertAllTokensEventuallyUnlocked() {
        Awaitility.await()
                .atMost(Duration.TEN_SECONDS)
                .pollInterval(Duration.FIVE_HUNDRED_MILLISECONDS)
                .untilAsserted(() -> assertThat(unlockedTokens).hasSameElementsAs(tokenList));
    }
}
