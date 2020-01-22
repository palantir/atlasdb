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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.util.concurrent.RateLimiter;
import com.google.protobuf.ByteString;
import com.palantir.atlasdb.autobatch.Autobatchers;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.timelock.config.TargetedSweepLockControlConfig.RateLimitConfig;
import com.palantir.atlasdb.timelock.config.TargetedSweepLockControlConfig.ShardAndThreadConfig;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.util.Pair;
import java.io.Closeable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

public final class TargetedSweepLockDecorator implements OrderedLocksDecorator, Closeable {

    @VisibleForTesting
    public static final int LOCK_ACQUIRES_PER_SECOND = 2;

    private static final LockDescriptor PREFIX = StringLockDescriptor.of("shard 0");
    private static final LockDescriptor NEXT_LEX_PREFIX = StringLockDescriptor.of("shard " + ('9' + 1));
    private static final ByteString CONSERVATIVE_SUFFIX =
            ByteString.copyFrom(StringLockDescriptor.of("CONSERVATIVE").getBytes());
    private static final ByteString THOROUGH_SUFFIX =
            ByteString.copyFrom(StringLockDescriptor.of("THOROUGH").getBytes());

    private final LoadingCache<Map.Entry<SweepStrategy, AsyncLock>, TargetedSweepAsyncLock> targetedSweepLockByDelegate;
    private final AutobatchingUnlocker unlockerAutobatcher;

    @VisibleForTesting
    TargetedSweepLockDecorator(
            Supplier<RateLimitConfig> targetedSweepRateLimiterConfig,
            ScheduledExecutorService timeoutExecutor,
            AutobatchingUnlocker unlocker) {
        this.unlockerAutobatcher = unlocker;
        targetedSweepLockByDelegate = Caffeine.newBuilder()
                .build(delegateLock -> new TargetedSweepAsyncLock(
                        delegateLock.getValue(),
                        createRateLimiter(targetedSweepRateLimiterConfig, delegateLock.getKey()),
                        () -> targetedSweepRateLimiterConfig.get().enabled(),
                        timeoutExecutor,
                        unlocker));
    }

    private static RateLimiter createRateLimiter(Supplier<RateLimitConfig> rateLimiterConfig, SweepStrategy strategy) {
        RateLimitConfig first = rateLimiterConfig.get();
        ShardAndThreadConfig shardAndThreadConfig = first.config();
        switch (strategy) {
            case CONSERVATIVE:
                return RateLimiter.create(shardAndThreadConfig.conservativePermitsPerSecond());
            case THOROUGH:
                return RateLimiter.create(shardAndThreadConfig.thoroughPermitsPerSecond());
            default:
                throw new AssertionError("this should not happen");
        }
    }

    static TargetedSweepLockDecorator create(
            Supplier<RateLimitConfig> targetedSweepRateLimitConfig,
            ScheduledExecutorService timeoutExecutor) {
        return new TargetedSweepLockDecorator(
                targetedSweepRateLimitConfig,
                timeoutExecutor,
                new AutobatchingUnlocker(unlocker()));
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
                locks.set(i, targetedSweepLock(descriptors.get(i), locks.get(i)));
            }
        }

        return OrderedLocks.fromOrderedList(locks);
    }

    private AsyncLock targetedSweepLock(LockDescriptor descriptor, AsyncLock lock) {
        ByteString descriptorByteString = ByteString.copyFrom(descriptor.getBytes());
        if (descriptorByteString.endsWith(CONSERVATIVE_SUFFIX)) {
            return targetedSweepLockByDelegate.get(Maps.immutableEntry(SweepStrategy.CONSERVATIVE, lock));
        } else if (descriptorByteString.endsWith(THOROUGH_SUFFIX)) {
            return targetedSweepLockByDelegate.get(Maps.immutableEntry(SweepStrategy.THOROUGH, lock));
        } else {
            return lock;
        }
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
