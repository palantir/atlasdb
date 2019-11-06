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

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.LongSupplier;

import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.lockwatch.LockWatchState;
import com.palantir.atlasdb.timelock.lock.watch.LockWatchingService;
import com.palantir.common.streams.KeyedStream;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.LockWatch;

public class LockWatchingServiceImpl implements LockWatchingService {
    private static final Logger log = LoggerFactory.getLogger(LockWatchingServiceImpl.class);
    private final LongSupplier timestampSupplier;
    private final ConcurrentMap<UUID, Set<LockDescriptor>> serviceToLocks = new ConcurrentHashMap<>();
    private final ConcurrentMap<LockDescriptor, LockWatch> lockWatches = new ConcurrentHashMap<>();
    private final ConcurrentMap<LockDescriptor, Integer> referenceCounter = new ConcurrentHashMap<>();
    private final Map<LockToken, TimestampedLockDescriptors> lockTokenInfo = Maps.newHashMap();

    public LockWatchingServiceImpl(LongSupplier timestampSupplier) {
        this.timestampSupplier = timestampSupplier;
    }

    @Override
    public synchronized void startWatching(UUID serviceId, Set<LockDescriptor> locksToWatch) {
        log.error("Starting watch for LD {}", locksToWatch);
        locksToWatch.forEach(lockDescriptor -> {
            boolean newWatch = updateWatchesForService(serviceId, lockDescriptor);
            maybeIncreaseReferenceCount(lockDescriptor, newWatch);
            lockWatches.putIfAbsent(lockDescriptor, LockWatch.INVALID);
        });
    }

    private void maybeIncreaseReferenceCount(LockDescriptor lockDescriptor, boolean newWatch) {
        if (newWatch) {
            referenceCounter.compute(lockDescriptor, (ignore, count) -> Optional.ofNullable(count).orElse(0) + 1);
        }
    }

    private boolean updateWatchesForService(UUID serviceId, LockDescriptor lockDescriptor) {
        return serviceToLocks.computeIfAbsent(serviceId, (ignore) -> Sets.newHashSet()).add(lockDescriptor);
    }

    @Override
    public synchronized void stopWatching(UUID serviceId, Set<LockDescriptor> locksToUnwatch) {
        maybeDecreaseReferenceCountAndCleanup(serviceId, locksToUnwatch);
    }

    private void maybeDecreaseReferenceCountAndCleanup(UUID serviceId, Set<LockDescriptor> lockDescriptors) {
        Set<LockDescriptor> currentWatches = serviceToLocks.getOrDefault(serviceId, ImmutableSet.of());
        Set<LockDescriptor> removedDescriptors = Sets.newHashSet();
        removedDescriptors.addAll(Sets.intersection(currentWatches, lockDescriptors));

        currentWatches.removeAll(lockDescriptors);
        removedDescriptors.forEach(lockDescriptor ->
            referenceCounter.compute(lockDescriptor, (no, count) -> decreaseCountWithCleanup(lockDescriptor, count)));
    }

    private Integer decreaseCountWithCleanup(LockDescriptor lockDescriptor, Integer count) {
        if (count > 1) {
            return count - 1;
        }
        lockWatches.remove(lockDescriptor);
        return null;
    }

    @Override
    public void registerLock(Set<LockDescriptor> locksTakenOut, LockToken lockToken) {
        long lockTimestamp = timestampSupplier.getAsLong();
        LockWatch newWatch = LockWatch.uncommitted(lockTimestamp);
        locksTakenOut.forEach(lockDescriptor -> updateToLatest(lockDescriptor, newWatch));
        lockTokenInfo.put(lockToken, ImmutableTimestampedLockDescriptors.of(lockTimestamp, locksTakenOut));
    }

    private void updateToLatest(LockDescriptor lockDescriptor, LockWatch newWatch) {
        lockWatches.computeIfPresent(lockDescriptor, (ignore, oldWatch) -> LockWatch.latest(oldWatch, newWatch));
    }

    @Override
    public void registerUnlock(LockToken lockToken) {
        lockTokenInfo.computeIfPresent(lockToken, (ignore, info) -> {
            info.lockDescriptors().forEach(lockDescriptor -> maybeCommit(lockDescriptor, info.timestamp()));
            return null;
        });
    }

    private void maybeCommit(LockDescriptor lockDescriptor, long timestamp) {
        lockWatches.computeIfPresent(lockDescriptor, (no, oldWatch) -> commitIfSameTimestamp(oldWatch, timestamp));
    }

    private LockWatch commitIfSameTimestamp(LockWatch oldWatch, long timestamp) {
        return oldWatch.timestamp() == timestamp ? LockWatch.committed(timestamp) : oldWatch;

    }

    @Override
    public LockWatchState getWatchState(UUID serviceId) {
        log.error("LWS {}", serviceToLocks.get(serviceId));
        return LockWatchState.of(KeyedStream.of(serviceToLocks.get(serviceId).stream())
                .map(lockDescriptor -> lockWatches.getOrDefault(lockDescriptor, LockWatch.INVALID))
                .collectToMap());
    }

    @Value.Immutable
    interface TimestampedLockDescriptors {
        @Value.Parameter
        long timestamp();
        @Value.Parameter
        Set<LockDescriptor> lockDescriptors();
    }
}
