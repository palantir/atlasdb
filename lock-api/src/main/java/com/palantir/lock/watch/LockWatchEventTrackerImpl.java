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

package com.palantir.lock.watch;

import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.google.common.primitives.Ints;
import com.palantir.lock.LockDescriptor;

public class LockWatchEventTrackerImpl implements LockWatchEventTracker {
    private ReadWriteLock lock = new ReentrantReadWriteLock();
    private AtomicReference<RangeSet<LockDescriptor>> watches = new AtomicReference<>(TreeRangeSet.create());
    private AtomicReference<Map<LockDescriptor, LockWatchInfo>> singleLocks = new AtomicReference<>(ImmutableMap.of());
    private final ConcurrentMap<Long, VersionedLockWatchState> startTsToLockWatchState = Maps.newConcurrentMap();
    private volatile OptionalLong lastKnownVersion = OptionalLong.empty();
    private volatile UUID leaderId = UUID.randomUUID();

    @Override
    public VersionedLockWatchState currentState() {
        lock.readLock().lock();
        try {
            return new VersionedLockWatchStateImpl(lastKnownVersion, watches.get(), singleLocks.get(), leaderId);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public synchronized VersionedLockWatchState updateState(LockWatchStateUpdate update) {
        if (leaderId != update.leaderId() || !lastKnownVersion.isPresent() || !update.success()) {
            resetAll(update);
            return currentState();
        }
        if (update.events().isEmpty() || update.lastKnownVersion().getAsLong() <= lastKnownVersion.getAsLong()) {
            return currentState();
        }

        TreeRangeSet<LockDescriptor> updatedWatches = TreeRangeSet.create(watches.get());
        Map<LockDescriptor, LockWatchInfo> updatedLocks = new HashMap<>(singleLocks.get());
        LockWatchStateEventVisitor visitor = new LockWatchStateEventVisitor(updatedWatches, updatedLocks);

        long firstVersion = update.events().get(0).sequence();
        update.events().subList(Ints.saturatedCast(lastKnownVersion.getAsLong() - firstVersion),
                update.events().size()).forEach(event -> event.accept(visitor));
        setAll(updatedWatches, updatedLocks, update.lastKnownVersion());
        return currentState();
    }

    @Override
    public void setLockWatchStateForStartTimestamp(long startTimestamp, VersionedLockWatchState lockWatchState) {
        startTsToLockWatchState.put(startTimestamp, lockWatchState);
    }

    @Override
    public VersionedLockWatchState getLockWatchStateForStartTimestamp(long startTimestamp) {
        return startTsToLockWatchState.get(startTimestamp);
    }

    @Override
    public VersionedLockWatchState removeLockWatchStateForStartTimestamp(long startTimestamp) {
        return startTsToLockWatchState.remove(startTimestamp);
    }

    private void resetAll(LockWatchStateUpdate update) {
        lock.writeLock().lock();
        try {
            watches.set(TreeRangeSet.create());
            singleLocks.set(ImmutableMap.of());
            lastKnownVersion = update.lastKnownVersion();
            leaderId = update.leaderId();
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void setAll(TreeRangeSet<LockDescriptor> updatedWatches, Map<LockDescriptor, LockWatchInfo> updatedLocks,
            OptionalLong version) {
        lock.writeLock().lock();
        try {
            watches.set(updatedWatches);
            singleLocks.set(updatedLocks);
            lastKnownVersion = version;
        } finally {
            lock.writeLock().unlock();
        }
    }
}
