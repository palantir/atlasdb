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

import java.io.Closeable;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BooleanSupplier;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Range;
import com.google.common.util.concurrent.RateLimiter;
import com.palantir.atlasdb.autobatch.Autobatchers;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.util.Pair;

final class TargetedSweepLockDecorator implements OrderedLocksDecorator, Closeable {

    private static final LockDescriptor PREFIX = StringLockDescriptor.of("shard 0");
    private static final LockDescriptor NEXT_LEX_PREFIX = StringLockDescriptor.of("shard :");

    private final LoadingCache<AsyncLock, TargetedSweepAsyncLock> targetedSweepRateLimitersById;
    private final AutobatchingUnlocker unlockerAutobatcher;

    @VisibleForTesting
    TargetedSweepLockDecorator(
            BooleanSupplier isRateLimiting,
            ScheduledExecutorService timeoutExecutor,
            AutobatchingUnlocker unlocker) {
        this.unlockerAutobatcher = unlocker;
        targetedSweepRateLimitersById = Caffeine.newBuilder()
                .build(delegateLock -> new TargetedSweepAsyncLock(
                        delegateLock,
                        RateLimiter.create(2),
                        isRateLimiting,
                        timeoutExecutor,
                        unlocker));
    }

    static TargetedSweepLockDecorator create(BooleanSupplier isRateLimiting, ScheduledExecutorService timeoutExecutor) {
        return new TargetedSweepLockDecorator(isRateLimiting, timeoutExecutor, new AutobatchingUnlocker(unlocker()));
    }

    private static final class AutobatchingUnlocker implements TargetedSweepLockUnlocker, Closeable {

        private final DisruptorAutobatcher<Pair<UUID, AsyncLock>, Void> autobatcher;

        AutobatchingUnlocker(DisruptorAutobatcher<Pair<UUID, AsyncLock>, Void> autobatcher) {
            this.autobatcher = autobatcher;
        }

        @Override
        public void unlock(AsyncLock lock, UUID requestId) {
            autobatcher.apply(Pair.create(requestId, lock));
        }

        @Override
        public void close() {
            autobatcher.close();
        }
    }

    static DisruptorAutobatcher<Pair<UUID, AsyncLock>, Void> unlocker() {
        return Autobatchers.<Pair<UUID, AsyncLock>, Void>independent(elements ->
            elements.forEach(element -> {
                AsyncLock lock = element.argument().rhSide;
                UUID requestId = element.argument().lhSide;
                lock.unlock(requestId);
                element.result().set(null);
            })
        )
                .safeLoggablePurpose("targeted-sweep-unlocker")
                .build();
    }

    @Override
    public OrderedLocks decorate(List<LockDescriptor> descriptors, List<AsyncLock> locks) {
        if (!containsTargetedSweepLocks(descriptors)) {
            return OrderedLocks.fromOrderedList(locks);
        }

        Range<Integer> targetedSweepLockRange = findTargetedSweepLocks(descriptors);
        if (!targetedSweepLockRange.isEmpty()) {
            for (int i = targetedSweepLockRange.lowerEndpoint(); i < targetedSweepLockRange.upperEndpoint(); i++) {
                TargetedSweepAsyncLock targetedSweepAsyncLock = targetedSweepRateLimitersById.get(locks.get(i));
                locks.set(i, targetedSweepAsyncLock);
            }
        }

        return OrderedLocks.fromOrderedList(locks);
    }

    private static boolean containsTargetedSweepLocks(List<LockDescriptor> descriptors) {
        int prefixIndex = index(Collections.binarySearch(descriptors, PREFIX));
        int nextLexIndex = index(Collections.binarySearch(descriptors, NEXT_LEX_PREFIX));

        return nextLexIndex > prefixIndex;
    }

    private static Range<Integer> findTargetedSweepLocks(List<LockDescriptor> descriptors) {
        int prefixIndex = index(Collections.binarySearch(descriptors, PREFIX));
        int nextLexIndex = index(Collections.binarySearch(descriptors, NEXT_LEX_PREFIX));

        return Range.openClosed(prefixIndex, nextLexIndex);
    }

    private static int index(int binarySearchResult) {
        return binarySearchResult >= 0 ? binarySearchResult : (binarySearchResult + 1) * -1;
    }

    @Override
    public void close() {
        unlockerAutobatcher.close();
    }
}
