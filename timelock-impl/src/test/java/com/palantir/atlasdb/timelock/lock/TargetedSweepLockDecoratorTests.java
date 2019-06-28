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

package com.palantir.atlasdb.timelock.lock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.util.Pair;

public class TargetedSweepLockDecoratorTests {

    private final TargetedSweepLockDecorator lockDecorator =
            TargetedSweepLockDecorator.create(() -> true, mock(ScheduledExecutorService.class));

    @After
    public void after() {
        lockDecorator.close();
    }

    @Test
    public void requestWithNoTargetedSweepLocksRemainsUntouched() {
        Pair<LockDescriptor, AsyncLock> nonTsLock1 = nonTargetedSweepLock();
        Pair<LockDescriptor, AsyncLock> nonTsLock2 = nonTargetedSweepLock();
        Pair<LockDescriptor, AsyncLock> nonTsLock3 = nonTargetedSweepLock();

        OrderedLocks locks = lockDecorator.decorate(
                descriptors(nonTsLock1, nonTsLock2, nonTsLock3),
                locks(nonTsLock1, nonTsLock2, nonTsLock3));

        assertThat(locks.get())
                .as("elements should be the same and in the same order")
                .containsExactlyElementsOf(locks(nonTsLock1, nonTsLock2, nonTsLock3));
    }

    @Test
    public void requestWithTargetedSweepLocksGetsTargetedSweepLock() {
        Pair<LockDescriptor, AsyncLock> tsLock = targetedSweepDescriptorWithRawLock(ShardAndStrategy.thorough(1));
        OrderedLocks locks = lockDecorator.decorate(
                ImmutableList.of(tsLock.getLhSide()),
                Lists.newArrayList(tsLock.getRhSide()));

        assertThat(locks.get())
                .as("this should be wrapped in a special targeted sweep lock")
                .allMatch(TargetedSweepAsyncLock.class::isInstance);
    }

    @Test
    public void repeatedRequestWithTargetedSweepLockGetsSameLock() {
        Pair<LockDescriptor, AsyncLock> tsLock = targetedSweepDescriptorWithRawLock(ShardAndStrategy.thorough(1));
        OrderedLocks locksForFirstRequest = lockDecorator.decorate(
                ImmutableList.of(tsLock.getLhSide()),
                Lists.newArrayList(tsLock.getRhSide()));

        OrderedLocks locksForSecondRequest = lockDecorator.decorate(
                ImmutableList.of(tsLock.getLhSide()),
                Lists.newArrayList(tsLock.getRhSide()));

        assertThat(locksForFirstRequest)
                .as("the locks should be exactly the same, since we're using it as a concurrency primitive")
                .isEqualTo(locksForSecondRequest);
    }

    @Test
    public void differentShardAndStrategyResultsInDifferentTargetedSweepLock() {
        Pair<LockDescriptor, AsyncLock> thoroughShard_1 =
                targetedSweepDescriptorWithRawLock(ShardAndStrategy.thorough(1));
        Pair<LockDescriptor, AsyncLock> conservativeShard_1 =
                targetedSweepDescriptorWithRawLock(ShardAndStrategy.conservative(1));
        Pair<LockDescriptor, AsyncLock> thoroughShard_2 =
                targetedSweepDescriptorWithRawLock(ShardAndStrategy.thorough(2));
        Pair<LockDescriptor, AsyncLock> conservativeShard_2 =
                targetedSweepDescriptorWithRawLock(ShardAndStrategy.conservative(2));

        OrderedLocks locks = lockDecorator.decorate(
                descriptors(thoroughShard_1, conservativeShard_1, thoroughShard_2, conservativeShard_2),
                locks(thoroughShard_1, conservativeShard_1, thoroughShard_2, conservativeShard_2));

        assertThat(locks.get())
                .hasSize(4)
                .as("these locks are all independent locks")
                .doesNotHaveDuplicates();
    }

    @Test
    public void mixingAndMatchingNonTsAndTsLocksWrapsTargetedSweepLocksCorrectly() {
        Pair<LockDescriptor, AsyncLock> nonTsLock1 = nonTargetedSweepLockWithPrefix("first");
        Pair<LockDescriptor, AsyncLock> nonTsLock2 = nonTargetedSweepLockWithPrefix("terminal lock coming after TS lock descriptor");
        Pair<LockDescriptor, AsyncLock> thorough = targetedSweepDescriptorWithRawLock(ShardAndStrategy.thorough(1));
        Pair<LockDescriptor, AsyncLock> conservative =
                targetedSweepDescriptorWithRawLock(ShardAndStrategy.conservative(1));

        OrderedLocks locks = lockDecorator.decorate(
                descriptors(nonTsLock1, thorough, conservative, nonTsLock2),
                locks(nonTsLock1, thorough, conservative, nonTsLock2));

        List<AsyncLock> lockList = locks.get();
        assertThat(Lists.newArrayList(lockList.get(0), lockList.get(3)))
                .as("the non-ts locks remain as non ts locks")
                .noneMatch(TargetedSweepAsyncLock.class::isInstance)
                .as("they should be exactly the same as the input locks")
                .containsExactly(nonTsLock1.getRhSide(), nonTsLock2.getRhSide());

        assertThat(Lists.newArrayList(lockList.subList(1, 3)))
                .as("the ts lock descriptors become ts async locks")
                .allMatch(TargetedSweepAsyncLock.class::isInstance)
                .as("they should not contain original input locks")
                .doesNotContain(thorough.getRhSide(), conservative.getRhSide());
    }

    private static Pair<LockDescriptor, AsyncLock> nonTargetedSweepLock() {
        return nonTargetedSweepLockWithPrefix("");
    }

    private static Pair<LockDescriptor, AsyncLock> nonTargetedSweepLockWithPrefix(String prefix) {
        return lockDescriptorWithMockAsyncLock(StringLockDescriptor.of(prefix + UUID.randomUUID().toString()));
    }

    private static Pair<LockDescriptor, AsyncLock> targetedSweepDescriptorWithRawLock(ShardAndStrategy shardStrategy) {
        return lockDescriptorWithMockAsyncLock(shardStrategy.toLockDescriptor());
    }

    private static Pair<LockDescriptor, AsyncLock> lockDescriptorWithMockAsyncLock(LockDescriptor descriptor) {
        return Pair.create(descriptor, mock(AsyncLock.class));
    }

    private static List<LockDescriptor> descriptors(Pair<LockDescriptor, AsyncLock>... pairs) {
        return Arrays.stream(pairs).map(Pair::getLhSide).collect(Collectors.toList());
    }

    private static List<AsyncLock> locks(Pair<LockDescriptor, AsyncLock>... pairs) {
        return Arrays.stream(pairs).map(Pair::getRhSide).collect(Collectors.toList());
    }
}
