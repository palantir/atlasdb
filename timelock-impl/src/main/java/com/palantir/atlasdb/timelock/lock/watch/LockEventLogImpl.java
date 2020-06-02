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

package com.palantir.atlasdb.timelock.lock.watch;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.collect.RangeSet;
import com.palantir.atlasdb.timelock.lock.AsyncLock;
import com.palantir.atlasdb.timelock.lock.HeldLocksCollection;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.IdentifiedVersion;
import com.palantir.lock.watch.LockEvent;
import com.palantir.lock.watch.LockWatchCreatedEvent;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.LockWatchReferences.LockWatchReference;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.lock.watch.UnlockEvent;

public class LockEventLogImpl implements LockEventLog {
    private final UUID logId;
    private final ArrayLockEventSlidingWindow slidingWindow = new ArrayLockEventSlidingWindow(1000);
    private final Supplier<LockWatches> watchesSupplier;
    private final HeldLocksCollection heldLocksCollection;

    LockEventLogImpl(UUID logId, Supplier<LockWatches> watchesSupplier, HeldLocksCollection heldLocksCollection) {
        this.logId = logId;
        this.watchesSupplier = watchesSupplier;
        this.heldLocksCollection = heldLocksCollection;
    }

    @Override
    public synchronized LockWatchStateUpdate getLogDiff(Optional<IdentifiedVersion> fromVersion) {
        if (shouldCalculateSnapshot(fromVersion)) {
            return calculateSnapshot();
        }
        List<LockWatchEvent> events = slidingWindow.getFromVersion(fromVersion.get().version());
        return LockWatchStateUpdate.success(logId, slidingWindow.lastVersion(), events);
    }

    @Override
    public synchronized <T> AtomicValue<T> runTaskAndAtomicallyReturnLockWatchStateUpdate(
            Optional<IdentifiedVersion> lastKnownVersion, Supplier<T> task) {
        T t = task.get();
        LockWatchStateUpdate logDiff = getLogDiff(lastKnownVersion);
        return AtomicValue.of(logDiff, t);
    }

    @Override
    public synchronized void logLock(Set<LockDescriptor> locksTakenOut, LockToken lockToken) {
        // I feel like this would be simpler if the isEmpty check was done somewhere else, it's weird API
        if (!locksTakenOut.isEmpty()) {
            synchronized (this) {
                slidingWindow.add(LockEvent.builder(locksTakenOut, lockToken));
            }
        }
    }

    @Override
    public void logUnlock(Set<LockDescriptor> locksUnlocked) {
        if (!locksUnlocked.isEmpty()) {
            synchronized (this) {
                slidingWindow.add(UnlockEvent.builder(locksUnlocked));
            }
        }
    }

    @Override
    public synchronized void logLockWatchCreated(LockWatches newWatches) {
        Set<LockDescriptor> openLocks = calculateOpenLocks(newWatches.ranges());
        slidingWindow.add(LockWatchCreatedEvent.builder(newWatches.references(), openLocks));
    }

    private boolean shouldCalculateSnapshot(Optional<IdentifiedVersion> fromVersion) {
        return !fromVersion.isPresent() || !fromVersion.get().id().equals(logId) || !slidingWindow.contains(
                fromVersion.get().version());
    }

    private LockWatchStateUpdate calculateSnapshot() {
        long lastVersion = slidingWindow.lastVersion();
        LockWatches currentWatches = watchesSupplier.get();
        Set<LockWatchReference> watches = new HashSet<>(currentWatches.references());
        Set<LockDescriptor> openLocks = calculateOpenLocks(currentWatches.ranges());
        return LockWatchStateUpdate.snapshot(
                logId,
                lastVersion,
                openLocks,
                watches);
    }

    /**
     * Iterates through all currently held locks and returns the set of all locks matching the watched ranges.
     * <p>
     * Note that the set of held locks can be modified during the execution of this method. Therefore, this method is
     * NOT guaranteed to return a consistent snapshot of the world. If the given set of ranges is being watched, i.e.,
     * every lock and unlock pertaining to watchedRanges is being appropriately logged, then any potential
     * inconsistencies can be corrected by replaying the events logged in the lock event log that occurred during
     * execution of this method. This is important to keep in mind if removing lock watches is something we wish to
     * implement.
     */
    private Set<LockDescriptor> calculateOpenLocks(RangeSet<LockDescriptor> watchedRanges) {
        return heldLocksCollection.locksHeld().stream()
                .flatMap(locksHeld -> locksHeld.getLocks().stream().map(AsyncLock::getDescriptor))
                .filter(watchedRanges::contains)
                .collect(Collectors.toSet());
    }
}
