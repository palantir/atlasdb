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
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.collect.RangeSet;
import com.palantir.atlasdb.timelock.lock.AsyncLock;
import com.palantir.atlasdb.timelock.lock.HeldLocksCollection;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.LockEvent;
import com.palantir.lock.watch.LockWatchCreatedEvent;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.LockWatchReferences.LockWatchReference;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.lock.watch.UnlockEvent;

public class LockEventLogImpl implements LockEventLog {
    private final UUID logId = UUID.randomUUID();
    private final ArrayLockEventSlidingWindow slidingWindow = new ArrayLockEventSlidingWindow(1000);
    private final Supplier<LockWatches> watchesSupplier;
    private final HeldLocksCollection heldLocksCollection;

    public LockEventLogImpl(Supplier<LockWatches> watchesSupplier, HeldLocksCollection heldLocksCollection) {
        this.watchesSupplier = watchesSupplier;
        this.heldLocksCollection = heldLocksCollection;
    }

    @Override
    public LockWatchStateUpdate getLogDiff(OptionalLong fromVersion) {
        if (!fromVersion.isPresent()) {
            return attemptToCalculateSnapshot();
        }
        Optional<List<LockWatchEvent>> maybeEvents = slidingWindow.getFromVersion(fromVersion.getAsLong());
        if (!maybeEvents.isPresent()) {
            return attemptToCalculateSnapshot();
        }
        List<LockWatchEvent> events = maybeEvents.get();
        return LockWatchStateUpdate.success(logId, fromVersion.getAsLong() + events.size(), events);
    }

    /**
     * Gets a snapshot estimate, then replays all lock watch events that occurred in the meantime on top of it
     * to correct any inconsistencies as discussed in {@link this#calculateOpenLocks(RangeSet)}.
     */
    private LockWatchStateUpdate attemptToCalculateSnapshot() {
        long startVersion = slidingWindow.lastVersion();
        SnapshotEventReplayer eventReplayer = getSnapshotEstimateAndCreateReplayer();
        long endVersion = slidingWindow.lastVersion();

        Optional<List<LockWatchEvent>> eventsToReplay = slidingWindow.getFromTo(startVersion, endVersion);
        if (!eventsToReplay.isPresent()) {
            return LockWatchStateUpdate.failed(logId);
        }
        eventsToReplay.get().forEach(eventReplayer::replay);
        return LockWatchStateUpdate.snapshot(
                logId,
                endVersion,
                eventReplayer.locked,
                eventReplayer.watches);
    }

    private SnapshotEventReplayer getSnapshotEstimateAndCreateReplayer() {
        LockWatches currentWatches = watchesSupplier.get();
        Set<LockWatchReference> watched = new HashSet<>(currentWatches.references());
        Set<LockDescriptor> estimatedLocks = calculateOpenLocks(currentWatches.ranges());
        return new SnapshotEventReplayer(watched, estimatedLocks);
    }

    /**
     * Iterates through all currently held locks and returns the set of all locks matching the watched ranges.
     *
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

    @Override
    public void logLock(Set<LockDescriptor> locksTakenOut, LockToken lockToken) {
        if (!locksTakenOut.isEmpty()) {
            slidingWindow.add(LockEvent.builder(locksTakenOut, lockToken));
        }
    }

    @Override
    public void logUnlock(Set<LockDescriptor> locksUnlocked) {
        if (!locksUnlocked.isEmpty()) {
            slidingWindow.add(UnlockEvent.builder(locksUnlocked));
        }
    }

    /**
     * Similar to {@link this#attemptToCalculateSnapshot()}, we get an estimate of open locks for the new watches,
     * then replay the recent events on top. Finally, we replay any additional events just before logging in a
     * synchronised block in
     * {@link ArrayLockEventSlidingWindow#finalizeAndAddSnapshot(long, LockWatchCreatedEventReplayer)}, ensuring nothing
     * is missed at logging time.
     */
    @Override
    public void logLockWatchCreated(LockWatches newWatches) {
        long startVersion = slidingWindow.lastVersion();
        LockWatchCreatedEventReplayer eventReplayer = getOpenLocksEstimateAndCreateReplayer(newWatches);
        long intermediaryVersion = slidingWindow.lastVersion();

        Optional<List<LockWatchEvent>> eventsToReplay = slidingWindow.getFromTo(startVersion, intermediaryVersion);
        if (!eventsToReplay.isPresent()) {
            // fail to log: this is fine, clients can request a snapshot if they need the info
            return;
        }
        eventsToReplay.get().forEach(eventReplayer::replay);
        slidingWindow.finalizeAndAddSnapshot(intermediaryVersion, eventReplayer);
    }

    private LockWatchCreatedEventReplayer getOpenLocksEstimateAndCreateReplayer(LockWatches newWatches) {
        Set<LockDescriptor> estimatedLocked = calculateOpenLocks(newWatches.ranges());
        return new LockWatchCreatedEventReplayer(newWatches, estimatedLocked);
    }

    /**
     * When replaying events on top of a snapshot, we must take into account all lock/unlock events as well as any
     * newly registered lock watches, so we can return the most accurate view of the world.
     */
    static class SnapshotEventReplayer implements LockWatchEvent.Visitor<Void> {
        private final Set<LockWatchReference> watches;
        private final Set<LockDescriptor> locked;

        private SnapshotEventReplayer(Set<LockWatchReference> watches, Set<LockDescriptor> maybeLocked) {
            this.watches = watches;
            this.locked = maybeLocked;
        }

        private void replay(LockWatchEvent event) {
            event.accept(this);
        }

        @Override
        public Void visit(LockEvent lockEvent) {
            locked.addAll(lockEvent.lockDescriptors());
            return null;
        }

        @Override
        public Void visit(UnlockEvent unlockEvent) {
            locked.removeAll(unlockEvent.lockDescriptors());
            return null;
        }

        @Override
        public Void visit(LockWatchCreatedEvent lockWatchCreatedEvent) {
            locked.addAll(lockWatchCreatedEvent.lockDescriptors());
            watches.addAll(lockWatchCreatedEvent.references());
            return null;
        }
    }

    /**
     * When replaying events for a lock watch created event, we only care about lock descriptors matching the newly
     * registered locks
     */
    static class LockWatchCreatedEventReplayer implements LockWatchEvent.Visitor<Void> {
        private final LockWatches lockWatches;
        private final Set<LockDescriptor> locked;

        private LockWatchCreatedEventReplayer(LockWatches watchesAndRanges, Set<LockDescriptor> maybeLocked) {
            this.lockWatches = watchesAndRanges;
            this.locked = maybeLocked;
        }

        void replay(LockWatchEvent event) {
            event.accept(this);
        }

        @Override
        public Void visit(LockEvent lockEvent) {
            lockEvent.lockDescriptors().stream().filter(lockWatches.ranges()::contains).forEach(locked::add);
            return null;
        }

        @Override
        public Void visit(UnlockEvent unlockEvent) {
            locked.removeAll(unlockEvent.lockDescriptors());
            return null;
        }

        @Override
        public Void visit(LockWatchCreatedEvent lockWatchCreatedEvent) {
            return null;
        }

        Set<LockWatchReference> getReferences() {
            return lockWatches.references();
        }

        Set<LockDescriptor> getLockedDescriptors() {
            return locked;
        }
    }
}
