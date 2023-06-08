/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockResponse;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.SimpleTimeDuration;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.ThreadAwareCloseableLockService;
import com.palantir.lock.ThreadAwareLockClient;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.awaitility.Awaitility;
import org.junit.Test;

public class ThreadAwareLockServiceImplTest {

    // Disable background thread info collection by default, it will be invoked manually instead
    private final LockServiceImpl lockService = LockServiceImpl.create(LockServerOptions.builder()
            .isStandaloneServer(false)
            .collectThreadInfo(false)
            .build());

    private final ExecutorService executor =
            PTExecutors.newCachedThreadPool(ThreadAwareLockServiceImplTest.class.getName());

    private static final LockDescriptor TEST_LOCK_1 = StringLockDescriptor.of("lock-1");
    private static final LockDescriptor TEST_LOCK_2 = StringLockDescriptor.of("lock-2");
    private static final LockDescriptor TEST_LOCK_3 = StringLockDescriptor.of("lock-3");

    private static final String TEST_THREAD_1 = "thread-1";
    private static final String TEST_THREAD_2 = "thread-2";
    private static final String TEST_THREAD_3 = "thread-3";

    private static final LockClient TEST_LOCK_CLIENT = LockClient.ANONYMOUS;

    @Test
    public void initialSnapShotIsEmpty() {
        lockService.updateThreadInfoSnapshot();
        assertThat(lockService.getLastKnownThreadInfoSnapshot()).isEmpty();
    }

    @Test
    public void collectsHeldLocks_singleLock() throws InterruptedException {
        LockRequest lockRequest = LockRequest.builder(ImmutableSortedMap.of(TEST_LOCK_1, LockMode.WRITE))
                .withCreatingThreadName(TEST_THREAD_1)
                .build();

        lockService.lockWithFullLockResponse(TEST_LOCK_CLIENT, lockRequest);
        lockService.updateThreadInfoSnapshot();

        assertThat(lockService.getLastKnownThreadInfoSnapshot())
                .containsExactly(Map.entry(TEST_LOCK_1, ThreadAwareLockClient.of(TEST_LOCK_CLIENT, TEST_THREAD_1)));
    }

    @Test
    public void collectsHeldLocks_multipleUniqueLocks_fromDifferentThreads() throws InterruptedException {
        final int numThreads = 10;
        final int numLocksPerThread = 10;

        List<String> threadNames =
                IntStream.range(0, numThreads).mapToObj(i -> "test-thread-" + i).collect(Collectors.toList());
        Map<String, List<LockDescriptor>> locksPerThread = new HashMap<>();
        for (String threadName : threadNames) {
            List<LockDescriptor> locks = IntStream.range(0, numLocksPerThread)
                    .mapToObj(i -> StringLockDescriptor.of("test-lock-" + i + "-from-thread-" + threadName))
                    .collect(Collectors.toList());
            locksPerThread.put(threadName, locks);
        }

        for (String threadName : threadNames) {
            LockRequest lockRequest = LockRequest.builder(
                            ImmutableSortedMap.copyOf(locksPerThread.get(threadName).stream()
                                    .collect(Collectors.toMap(lock -> lock, lock -> LockMode.WRITE))))
                    .withCreatingThreadName(threadName)
                    .build();
            lockService.lockWithFullLockResponse(TEST_LOCK_CLIENT, lockRequest);
        }
        lockService.updateThreadInfoSnapshot();

        Map<LockDescriptor, ThreadAwareLockClient> expected = locksPerThread.keySet().stream()
                .flatMap(threadName -> locksPerThread.get(threadName).stream()
                        .map(lock -> Map.entry(lock, ThreadAwareLockClient.of(TEST_LOCK_CLIENT, threadName))))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        assertThat(lockService.getLastKnownThreadInfoSnapshot()).containsExactlyInAnyOrderEntriesOf(expected);
    }

    @Test
    public void collectsHeldLocks_sharedLock_fromDifferentThreads() throws InterruptedException {
        LockRequest lockRequest1 = LockRequest.builder(ImmutableSortedMap.of(TEST_LOCK_1, LockMode.READ))
                .withCreatingThreadName(TEST_THREAD_1)
                .build();
        LockRequest lockRequest2 = LockRequest.builder(ImmutableSortedMap.of(TEST_LOCK_1, LockMode.READ))
                .doNotBlock()
                .lockAsManyAsPossible()
                .withCreatingThreadName(TEST_THREAD_2)
                .build();

        lockService.lockWithFullLockResponse(TEST_LOCK_CLIENT, lockRequest1);
        lockService.lockWithFullLockResponse(TEST_LOCK_CLIENT, lockRequest2);
        lockService.updateThreadInfoSnapshot();

        assertThat(lockService.getLastKnownThreadInfoSnapshot())
                .hasSize(1)
                .containsAnyOf(
                        Map.entry(TEST_LOCK_1, ThreadAwareLockClient.of(TEST_LOCK_CLIENT, TEST_THREAD_1)),
                        Map.entry(TEST_LOCK_1, ThreadAwareLockClient.of(TEST_LOCK_CLIENT, TEST_THREAD_2)));
    }

    @Test
    public void doesNotCollectDroppedLocksWithLockAllOrNone() throws InterruptedException {
        LockRequest lockRequest1 = LockRequest.builder(ImmutableSortedMap.of(TEST_LOCK_1, LockMode.WRITE))
                .withCreatingThreadName(TEST_THREAD_1)
                .build();

        // T2 cannot get lock 1 -> grouping behavior LOCK_ALL_OR_NONE will release lock 2, even though it could have
        // been locked
        LockRequest lockRequest2 = LockRequest.builder(
                        ImmutableSortedMap.of(TEST_LOCK_1, LockMode.READ, TEST_LOCK_2, LockMode.READ))
                .doNotBlock()
                .withCreatingThreadName(TEST_THREAD_2)
                .build();

        lockService.lockWithFullLockResponse(TEST_LOCK_CLIENT, lockRequest1);
        lockService.lockWithFullLockResponse(TEST_LOCK_CLIENT, lockRequest2);
        lockService.updateThreadInfoSnapshot();

        assertThat(lockService.getLastKnownThreadInfoSnapshot())
                .containsExactly(Map.entry(TEST_LOCK_1, ThreadAwareLockClient.of(TEST_LOCK_CLIENT, TEST_THREAD_1)));
    }

    @Test
    public void doesNotCollectUnlockedLocks() throws InterruptedException {
        LockRequest lockRequest = LockRequest.builder(ImmutableSortedMap.of(TEST_LOCK_1, LockMode.WRITE))
                .withCreatingThreadName(TEST_THREAD_1)
                .build();

        LockResponse response = lockService.lockWithFullLockResponse(TEST_LOCK_CLIENT, lockRequest);
        lockService.unlock(response.getLockRefreshToken());
        lockService.updateThreadInfoSnapshot();

        assertThat(lockService.getLastKnownThreadInfoSnapshot()).isEmpty();
    }

    @Test
    public void doesNotCollectFailedLocks() throws InterruptedException {
        LockRequest lockRequest1 = LockRequest.builder(ImmutableSortedMap.of(TEST_LOCK_1, LockMode.WRITE))
                .withCreatingThreadName(TEST_THREAD_1)
                .build();
        LockRequest lockRequest2 = LockRequest.builder(ImmutableSortedMap.of(TEST_LOCK_1, LockMode.WRITE))
                .doNotBlock()
                .lockAsManyAsPossible()
                .withCreatingThreadName(TEST_THREAD_2)
                .build();

        LockResponse response = lockService.lockWithFullLockResponse(TEST_LOCK_CLIENT, lockRequest1);
        // should fail to acquire lock
        lockService.lockWithFullLockResponse(TEST_LOCK_CLIENT, lockRequest2);
        // After unlocking locks of T1, no more locks should be held by any thread
        lockService.unlock(response.getLockRefreshToken());
        lockService.updateThreadInfoSnapshot();

        assertThat(lockService.getLastKnownThreadInfoSnapshot()).isEmpty();
    }

    @Test
    public void collectsCorrectStateAfterMultipleOperations() throws Exception {
        final CyclicBarrier barrier = new CyclicBarrier(2);

        // T1 locks 1 exclusively and locks 2 in shared mode
        LockResponse response1 = lockService.lockWithFullLockResponse(
                TEST_LOCK_CLIENT,
                LockRequest.builder(ImmutableSortedMap.of(TEST_LOCK_1, LockMode.WRITE, TEST_LOCK_2, LockMode.READ))
                        .doNotBlock()
                        .lockAsManyAsPossible()
                        .withCreatingThreadName(TEST_THREAD_1)
                        .build());

        // T2 won't get lock 1, but locks 3 in exclusive mode,
        LockResponse response2 = lockService.lockWithFullLockResponse(
                TEST_LOCK_CLIENT,
                LockRequest.builder(ImmutableSortedMap.of(TEST_LOCK_1, LockMode.READ, TEST_LOCK_3, LockMode.WRITE))
                        .doNotBlock()
                        .lockAsManyAsPossible()
                        .withCreatingThreadName(TEST_THREAD_2)
                        .build());

        executor.submit((Callable<Void>) () -> {

            // T3 will wait 5 seconds to lock 1 in exclusive mode and lock 2 in shared mode
            LockResponse response3 = lockService.lockWithFullLockResponse(
                    TEST_LOCK_CLIENT,
                    LockRequest.builder(ImmutableSortedMap.of(TEST_LOCK_1, LockMode.WRITE, TEST_LOCK_2, LockMode.READ))
                            .blockForAtMost(SimpleTimeDuration.of(5, TimeUnit.SECONDS))
                            .lockAsManyAsPossible()
                            .withCreatingThreadName(TEST_THREAD_3)
                            .build());
            barrier.await();
            return null;
        });
        lockService.unlock(response1.getLockRefreshToken());
        barrier.await();

        lockService.updateThreadInfoSnapshot();

        // Now T1 should hold nothing, T2 holds 3 in exclusive mode, T3 holds 1 in exclusive and 2 in shared mode
        Map<LockDescriptor, ThreadAwareLockClient> expected = ImmutableMap.of(
                TEST_LOCK_1,
                ThreadAwareLockClient.of(TEST_LOCK_CLIENT, TEST_THREAD_3),
                TEST_LOCK_2,
                ThreadAwareLockClient.of(TEST_LOCK_CLIENT, TEST_THREAD_3),
                TEST_LOCK_3,
                ThreadAwareLockClient.of(TEST_LOCK_CLIENT, TEST_THREAD_2));

        assertThat(lockService.getLastKnownThreadInfoSnapshot()).containsExactlyInAnyOrderEntriesOf(expected);
    }

    @Test
    public void backgroundCollectionWorks() throws InterruptedException {
        final ThreadAwareCloseableLockService lockWithServiceWithBackgroundRunner =
                LockServiceImpl.create(LockServerOptions.builder()
                        .isStandaloneServer(false)
                        .collectThreadInfo(true)
                        .threadInfoSnapshotInterval(SimpleTimeDuration.of(100, TimeUnit.MILLISECONDS))
                        .build());

        LockRequest lockRequest = LockRequest.builder(ImmutableSortedMap.of(TEST_LOCK_1, LockMode.WRITE))
                .withCreatingThreadName(TEST_THREAD_1)
                .build();

        lockWithServiceWithBackgroundRunner.lockWithFullLockResponse(TEST_LOCK_CLIENT, lockRequest);

        Awaitility.await("wait for background runner to take a snapshot")
                .atMost(500, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    assertThat(lockWithServiceWithBackgroundRunner.getLastKnownThreadInfoSnapshot())
                            .containsExactly(
                                    Map.entry(TEST_LOCK_1, ThreadAwareLockClient.of(TEST_LOCK_CLIENT, TEST_THREAD_1)));
                });
    }
}
