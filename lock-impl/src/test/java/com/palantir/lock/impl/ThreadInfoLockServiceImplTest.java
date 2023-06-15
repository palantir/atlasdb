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

import com.google.common.collect.ImmutableSortedMap;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.streams.KeyedStream;
import com.palantir.lock.DebugThreadInfoConfiguration;
import com.palantir.lock.ImmutableDebugThreadInfoConfiguration;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockClientAndThread;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockResponse;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.SimpleTimeDuration;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.refreshable.Refreshable;
import com.palantir.refreshable.SettableRefreshable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.awaitility.Awaitility;
import org.junit.Test;

public class ThreadInfoLockServiceImplTest {
    private static final LockDescriptor TEST_LOCK_1 = StringLockDescriptor.of("lock-1");
    private static final LockDescriptor TEST_LOCK_2 = StringLockDescriptor.of("lock-2");
    private static final LockDescriptor TEST_LOCK_3 = StringLockDescriptor.of("lock-3");

    private static final String TEST_THREAD_1 = "thread-1";
    private static final String TEST_THREAD_2 = "thread-2";
    private static final String TEST_THREAD_3 = "thread-3";

    private static final LockRequest LOCK_1_THREAD_1_WRITE_REQUEST = LockRequest.builder(
                    ImmutableSortedMap.of(TEST_LOCK_1, LockMode.WRITE))
            .withCreatingThreadName(TEST_THREAD_1)
            .build();

    private static final LockClientAndThread ANONYMOUS_TEST_THREAD_1 =
            LockClientAndThread.of(LockClient.ANONYMOUS, TEST_THREAD_1);

    private final ExecutorService executor =
            PTExecutors.newCachedThreadPool(ThreadInfoLockServiceImplTest.class.getName());

    // Disable background snapshotting, invoke explicitly instead
    private final LockServiceImpl lockService = LockServiceImpl.create(LockServerOptions.builder()
            .isStandaloneServer(false)
            .threadInfoConfiguration(Refreshable.only(ImmutableDebugThreadInfoConfiguration.builder()
                    .recordThreadInfo(false)
                    .build()))
            .build());

    @Test
    public void initialThreadInfoIsEmpty() {
        forceSnapshot();
        assertThat(getLatestThreadInfoForLock(TEST_LOCK_1)).isEmpty();
        assertThat(getLatestThreadInfoForLock(TEST_LOCK_2)).isEmpty();
        assertThat(getLatestThreadInfoForLock(TEST_LOCK_3)).isEmpty();
    }

    @Test
    public void recordsThreadInfoForLock() throws InterruptedException {
        lockService.lockWithFullLockResponse(LockClient.ANONYMOUS, LOCK_1_THREAD_1_WRITE_REQUEST);
        forceSnapshot();

        assertThat(getLatestThreadInfoForLock(TEST_LOCK_1)).contains(ANONYMOUS_TEST_THREAD_1);
    }

    @Test
    public void recordsThreadInfoForUnlock() throws InterruptedException {
        LockResponse response =
                lockService.lockWithFullLockResponse(LockClient.ANONYMOUS, LOCK_1_THREAD_1_WRITE_REQUEST);
        forceSnapshot();

        assertThat(getLatestThreadInfoForLock(TEST_LOCK_1)).contains(ANONYMOUS_TEST_THREAD_1);

        lockService.unlock(response.getToken());
        forceSnapshot();

        assertThat(getLatestThreadInfoForLock(TEST_LOCK_1)).isEmpty();
    }

    @Test
    public void recordsThreadInfoForMultipleLocks() throws InterruptedException {
        int numThreads = 10;
        int numLocksPerThread = 10;

        List<String> threadNames =
                IntStream.range(0, numThreads).mapToObj(i -> "test-thread-" + i).collect(Collectors.toList());

        Map<String, Set<LockDescriptor>> locksPerThread = KeyedStream.of(threadNames)
                .map(threadName -> IntStream.range(0, numLocksPerThread)
                        .mapToObj(i -> StringLockDescriptor.of("test-lock-" + i + "-from-thread-" + threadName))
                        .collect(Collectors.toSet()))
                .collectToMap();

        for (Map.Entry<String, Set<LockDescriptor>> entry : locksPerThread.entrySet()) {
            LockRequest lockRequest = LockRequest.builder(ImmutableSortedMap.copyOf(
                            entry.getValue().stream().collect(Collectors.toMap(lock -> lock, lock -> LockMode.WRITE))))
                    .withCreatingThreadName(entry.getKey())
                    .build();
            lockService.lockWithFullLockResponse(LockClient.ANONYMOUS, lockRequest);
        }

        forceSnapshot();

        locksPerThread.forEach((threadName, lockDescriptors) ->
                lockDescriptors.forEach(lock -> assertThat(getLatestThreadInfoForLock(lock))
                        .contains(LockClientAndThread.of(LockClient.ANONYMOUS, threadName))));
    }

    @Test
    public void recordsThreadInfoForSharedLock() throws InterruptedException {
        LockRequest lockRequest1 = LockRequest.builder(ImmutableSortedMap.of(TEST_LOCK_1, LockMode.READ))
                .withCreatingThreadName(TEST_THREAD_1)
                .build();
        LockRequest lockRequest2 = LockRequest.builder(ImmutableSortedMap.of(TEST_LOCK_1, LockMode.READ))
                .doNotBlock()
                .lockAsManyAsPossible()
                .withCreatingThreadName(TEST_THREAD_2)
                .build();

        LockResponse lockResponse = lockService.lockWithFullLockResponse(LockClient.ANONYMOUS, lockRequest1);
        lockService.lockWithFullLockResponse(LockClient.ANONYMOUS, lockRequest2);

        forceSnapshot();

        assertThat(getLatestThreadInfoForLock(TEST_LOCK_1).get())
                .isIn(ANONYMOUS_TEST_THREAD_1, LockClientAndThread.of(LockClient.ANONYMOUS, TEST_THREAD_2));

        lockService.unlock(lockResponse.getToken());

        forceSnapshot();

        assertThat(getLatestThreadInfoForLock(TEST_LOCK_1))
                .contains(LockClientAndThread.of(LockClient.ANONYMOUS, TEST_THREAD_2));
    }

    @Test
    public void doesNotRecordDroppedLocksWithLockAllOrNone() throws InterruptedException {
        // T2 cannot get lock 1 -> grouping behavior LOCK_ALL_OR_NONE will release lock 2, even though it could have
        // been locked
        LockRequest lockRequest2 = LockRequest.builder(
                        ImmutableSortedMap.of(TEST_LOCK_1, LockMode.READ, TEST_LOCK_2, LockMode.READ))
                .doNotBlock()
                .withCreatingThreadName(TEST_THREAD_2)
                .build();

        lockService.lockWithFullLockResponse(LockClient.ANONYMOUS, LOCK_1_THREAD_1_WRITE_REQUEST);
        lockService.lockWithFullLockResponse(LockClient.ANONYMOUS, lockRequest2);
        forceSnapshot();

        assertThat(getLatestThreadInfoForLock(TEST_LOCK_1)).contains(ANONYMOUS_TEST_THREAD_1);
        assertThat(getLatestThreadInfoForLock(TEST_LOCK_2)).isEmpty();
    }

    @Test
    public void recordsUnlockAndFreezeForNonAnonymousClient() throws InterruptedException {
        LockClient client = LockClient.of("non-anonymous-client");

        LockResponse response = lockService.lockWithFullLockResponse(client, LOCK_1_THREAD_1_WRITE_REQUEST);
        forceSnapshot();

        assertThat(getLatestThreadInfoForLock(TEST_LOCK_1)).contains(LockClientAndThread.of(client, TEST_THREAD_1));

        lockService.unlockAndFreeze(response.getToken());
        forceSnapshot();

        assertThat(getLatestThreadInfoForLock(TEST_LOCK_1)).isEmpty();
    }

    @Test
    public void recordsCorrectStateAfterMultipleOperations() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        // T1 locks 1 exclusively and locks 2 in shared mode
        LockResponse response1 = lockService.lockWithFullLockResponse(
                LockClient.ANONYMOUS,
                LockRequest.builder(ImmutableSortedMap.of(TEST_LOCK_1, LockMode.WRITE, TEST_LOCK_2, LockMode.READ))
                        .doNotBlock()
                        .lockAsManyAsPossible()
                        .withCreatingThreadName(TEST_THREAD_1)
                        .build());

        // T2 shouldn't get 1, but locks 3 in exclusive mode,
        LockResponse response2 = lockService.lockWithFullLockResponse(
                LockClient.ANONYMOUS,
                LockRequest.builder(ImmutableSortedMap.of(TEST_LOCK_1, LockMode.READ, TEST_LOCK_3, LockMode.WRITE))
                        .doNotBlock()
                        .lockAsManyAsPossible()
                        .withCreatingThreadName(TEST_THREAD_2)
                        .build());

        Future<LockResponse> response3 = executor.submit(() -> {

            // T3 will wait at most 5 seconds to lock 1 in exclusive mode and lock 2 in shared mode
            LockResponse response = lockService.lockWithFullLockResponse(
                    LockClient.ANONYMOUS,
                    LockRequest.builder(ImmutableSortedMap.of(TEST_LOCK_1, LockMode.WRITE, TEST_LOCK_2, LockMode.READ))
                            .blockForAtMost(SimpleTimeDuration.of(5, TimeUnit.SECONDS))
                            .lockAsManyAsPossible()
                            .withCreatingThreadName(TEST_THREAD_3)
                            .build());
            latch.countDown();
            return response;
        });
        lockService.unlock(response1.getToken());
        latch.await();

        forceSnapshot();

        // T1 should hold nothing, T2 holds 3 in exclusive mode, T3 holds 1 in exclusive and 2 in shared mode
        assertThat(getLatestThreadInfoForLock(TEST_LOCK_1))
                .contains(LockClientAndThread.of(LockClient.ANONYMOUS, TEST_THREAD_3));
        assertThat(getLatestThreadInfoForLock(TEST_LOCK_2))
                .contains(LockClientAndThread.of(LockClient.ANONYMOUS, TEST_THREAD_3));
        assertThat(getLatestThreadInfoForLock(TEST_LOCK_3))
                .contains(LockClientAndThread.of(LockClient.ANONYMOUS, TEST_THREAD_2));

        lockService.unlock(response2.getToken());
        forceSnapshot();

        // Only T3 should hold locks
        assertThat(getLatestThreadInfoForLock(TEST_LOCK_1))
                .contains(LockClientAndThread.of(LockClient.ANONYMOUS, TEST_THREAD_3));
        assertThat(getLatestThreadInfoForLock(TEST_LOCK_2))
                .contains(LockClientAndThread.of(LockClient.ANONYMOUS, TEST_THREAD_3));
        assertThat(getLatestThreadInfoForLock(TEST_LOCK_3)).isEmpty();

        lockService.unlock(response3.get().getToken());
        forceSnapshot();

        assertThat(getLatestThreadInfoForLock(TEST_LOCK_1)).isEmpty();
        assertThat(getLatestThreadInfoForLock(TEST_LOCK_2)).isEmpty();
        assertThat(getLatestThreadInfoForLock(TEST_LOCK_3)).isEmpty();
    }

    @Test
    public void refreshDoesNotChangeSnapshot() throws InterruptedException {
        LockResponse response =
                lockService.lockWithFullLockResponse(LockClient.ANONYMOUS, LOCK_1_THREAD_1_WRITE_REQUEST);
        forceSnapshot();

        assertThat(getLatestThreadInfoForLock(TEST_LOCK_1)).contains(ANONYMOUS_TEST_THREAD_1);

        lockService.refreshTokens(List.of(response.getToken()));
        forceSnapshot();

        assertThat(getLatestThreadInfoForLock(TEST_LOCK_1)).contains(ANONYMOUS_TEST_THREAD_1);
    }

    @Test
    public void backgroundSnapshotRunnerWorks() throws InterruptedException {
        LockServiceImpl lockServiceWithBackgroundRunner = LockServiceImpl.create(LockServerOptions.builder()
                .isStandaloneServer(false)
                .threadInfoConfiguration(Refreshable.only(ImmutableDebugThreadInfoConfiguration.builder()
                        .recordThreadInfo(true)
                        .threadInfoSnapshotIntervalMillis(50L)
                        .build()))
                .build());
        LockThreadInfoSnapshotManager backgroundSnapshotRunner = lockServiceWithBackgroundRunner.getSnapshotManager();

        LockResponse response = lockServiceWithBackgroundRunner.lockWithFullLockResponse(
                LockClient.ANONYMOUS, LOCK_1_THREAD_1_WRITE_REQUEST);

        Awaitility.await().atMost(200, TimeUnit.MILLISECONDS).untilAsserted(() -> assertThat(
                        backgroundSnapshotRunner.getLastKnownThreadInfoSnapshot())
                .containsExactly(Map.entry(TEST_LOCK_1, ANONYMOUS_TEST_THREAD_1)));

        lockServiceWithBackgroundRunner.unlock(response.getToken());

        Awaitility.await().atMost(200, TimeUnit.MILLISECONDS).untilAsserted(() -> assertThat(
                        backgroundSnapshotRunner.getLastKnownThreadInfoSnapshot())
                .doesNotContainKey(TEST_LOCK_1));
    }

    @Test
    public void logArgIsUnsafeAndContainsEmptyOptionalIfNotRecordingThreadInfo() throws InterruptedException {
        lockService.lockWithFullLockResponse(LockClient.ANONYMOUS, LOCK_1_THREAD_1_WRITE_REQUEST);
        forceSnapshot();
        assertThat(lockService.getSnapshotManager().getRestrictedSnapshotAsOptionalLogArg(Set.of(TEST_LOCK_1)))
                .isEqualTo(UnsafeArg.of("presumedClientThreadHoldersIfEnabled", Optional.empty()));
    }

    @Test
    public void logArgIsUnsafeAndContainsRestrictedSnapshotIfRecordingThreadInfo() throws InterruptedException {
        LockServiceImpl lockServiceWithBackgroundRunner = LockServiceImpl.create(LockServerOptions.builder()
                .isStandaloneServer(false)
                .threadInfoConfiguration(Refreshable.only(ImmutableDebugThreadInfoConfiguration.builder()
                        .recordThreadInfo(true)
                        .threadInfoSnapshotIntervalMillis(50L)
                        .build()))
                .build());
        LockRequest lockRequest2 = LockRequest.builder(
                        ImmutableSortedMap.of(TEST_LOCK_1, LockMode.READ, TEST_LOCK_2, LockMode.READ))
                .doNotBlock()
                .withCreatingThreadName(TEST_THREAD_2)
                .build();

        lockServiceWithBackgroundRunner.lockWithFullLockResponse(LockClient.ANONYMOUS, LOCK_1_THREAD_1_WRITE_REQUEST);
        lockServiceWithBackgroundRunner.lockWithFullLockResponse(LockClient.ANONYMOUS, lockRequest2);

        Awaitility.waitAtMost(200, TimeUnit.MILLISECONDS).untilAsserted(() -> assertThat(lockServiceWithBackgroundRunner
                        .getSnapshotManager()
                        .getRestrictedSnapshotAsOptionalLogArg(Set.of(TEST_LOCK_1)))
                .isEqualTo(UnsafeArg.of(
                        "presumedClientThreadHoldersIfEnabled",
                        // Snapshot should be restricted to TEST_LOCK_1
                        Optional.of(
                                Map.of(TEST_LOCK_1, LockClientAndThread.of(LockClient.ANONYMOUS, TEST_THREAD_1))))));
    }

    @Test
    public void willRestartWhenConfigIsUpdated() throws InterruptedException {
        SettableRefreshable<DebugThreadInfoConfiguration> refreshableThreadInfoConfiguration =
                Refreshable.create(ImmutableDebugThreadInfoConfiguration.builder()
                        .recordThreadInfo(true)
                        .threadInfoSnapshotIntervalMillis(50L)
                        .build());
        LockServiceImpl lockServiceWithBackgroundRunner = LockServiceImpl.create(LockServerOptions.builder()
                .isStandaloneServer(false)
                .threadInfoConfiguration(refreshableThreadInfoConfiguration)
                .build());
        LockThreadInfoSnapshotManager backgroundSnapshotRunner = lockServiceWithBackgroundRunner.getSnapshotManager();

        LockResponse lockResponse = lockServiceWithBackgroundRunner.lockWithFullLockResponse(
                LockClient.ANONYMOUS, LOCK_1_THREAD_1_WRITE_REQUEST);

        // background task is running and should take a snapshot
        Awaitility.await().atMost(200, TimeUnit.MILLISECONDS).untilAsserted(() -> assertThat(
                        backgroundSnapshotRunner.getLastKnownThreadInfoSnapshot())
                .containsExactly(Map.entry(TEST_LOCK_1, ANONYMOUS_TEST_THREAD_1)));

        // turn off background task
        refreshableThreadInfoConfiguration.update(ImmutableDebugThreadInfoConfiguration.builder()
                .recordThreadInfo(false)
                .threadInfoSnapshotIntervalMillis(50L)
                .build());

        Awaitility.await().until(() -> !backgroundSnapshotRunner.isRunning());

        lockServiceWithBackgroundRunner.unlock(lockResponse.getToken());

        // If background task was still running, it would take a snapshot at this moment
        Awaitility.await().atLeast(200, TimeUnit.MILLISECONDS);

        // ...but we expect it to do nothing
        assertThat(lockServiceWithBackgroundRunner.getSnapshotManager().getLastKnownThreadInfoSnapshot())
                .containsExactly(Map.entry(TEST_LOCK_1, ANONYMOUS_TEST_THREAD_1));

        // turn background task back on
        refreshableThreadInfoConfiguration.update(ImmutableDebugThreadInfoConfiguration.builder()
                .recordThreadInfo(true)
                .threadInfoSnapshotIntervalMillis(50L)
                .build());

        // Now the unlock request should be reflected in the snapshot
        Awaitility.await().atMost(200, TimeUnit.MILLISECONDS).untilAsserted(() -> assertThat(
                        backgroundSnapshotRunner.getLastKnownThreadInfoSnapshot())
                .doesNotContainKey(TEST_LOCK_1));
    }

    @Test
    public void closesWithLockService() {
        lockService.close();
        assertThat(lockService.getSnapshotManager().isClosed()).isTrue();
    }

    private void forceSnapshot() {
        lockService.getSnapshotManager().takeSnapshot();
    }

    private Optional<LockClientAndThread> getLatestThreadInfoForLock(LockDescriptor lock) {
        return Optional.ofNullable(lockService
                .getSnapshotManager()
                .getLastKnownThreadInfoSnapshot()
                .get(lock));
    }
}
