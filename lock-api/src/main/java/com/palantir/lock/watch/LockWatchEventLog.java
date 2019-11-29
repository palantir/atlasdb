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
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.google.common.primitives.Ints;
import com.palantir.lock.LockDescriptor;

public class LockWatchEventLog {
    private ReadWriteLock lock = new ReentrantReadWriteLock();
    private AtomicReference<RangeSet<LockDescriptor>> watches = new AtomicReference<>(TreeRangeSet.create());
    private AtomicReference<Map<LockDescriptor, LockWatchState>> singleLocks = new AtomicReference<>(ImmutableMap.of());
    private volatile OptionalLong lastKnownVersion = OptionalLong.empty();
    private volatile UUID leaderId = UUID.randomUUID();

    public VersionedLockWatchState currentState() {
        try {
            lock.readLock().lock();
            return new VersionedLockWatchStateImpl(lastKnownVersion, watches.get(), singleLocks.get(), leaderId);
        } finally {
            lock.readLock().unlock();
        }
    }

    public synchronized VersionedLockWatchState updateState(LockWatchStateUpdate update) {
            if (leaderId != update.leaderId() || !lastKnownVersion.isPresent() || !update.success()) {
                resetAll(update);
                return currentState();
            }
            if (update.events().isEmpty() || update.lastKnownVersion().getAsLong() <= lastKnownVersion.getAsLong()) {
                return currentState();
            }

            TreeRangeSet<LockDescriptor> updatedWatches = TreeRangeSet.create(watches.get());
            Map<LockDescriptor, LockWatchState> updatedLocks = new HashMap<>(singleLocks.get());
            LockWatchStateEventVisitor visitor = new LockWatchStateEventVisitor(updatedWatches, updatedLocks);

            long firstVersion = update.events().get(0).sequence();
            update.events().subList(Ints.saturatedCast(lastKnownVersion.getAsLong() - firstVersion),
                    update.events().size()).forEach(event -> event.accept(visitor));
            setAll(updatedWatches, updatedLocks, update.lastKnownVersion());
            return currentState();
    }

    private void resetAll(LockWatchStateUpdate update) {
        try {
            lock.writeLock().lock();
            watches.set(TreeRangeSet.create());
            singleLocks.set(ImmutableMap.of());
            lastKnownVersion = update.lastKnownVersion();
            leaderId = update.leaderId();
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void setAll(TreeRangeSet<LockDescriptor> updatedWatches, Map<LockDescriptor, LockWatchState> updatedLocks,
            OptionalLong version) {
        try {
            lock.writeLock().lock();
            watches.set(updatedWatches);
            singleLocks.set(updatedLocks);
            lastKnownVersion = version;
        } finally {
            lock.writeLock().unlock();
        }
    }
}
