/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.api.watch;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.watch.IdentifiedVersion;
import com.palantir.lock.watch.LockEvent;
import com.palantir.lock.watch.LockWatchCreatedEvent;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.LockWatchReferences;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.lock.watch.UnlockEvent;
import com.palantir.logsafe.Preconditions;

final class ClientLockWatchSnapshotImpl implements ClientLockWatchSnapshot {
    private final Set<LockWatchReferences.LockWatchReference> watches;
    private final Set<LockDescriptor> locked;
    private final EventVisitor visitor;
    private Optional<IdentifiedVersion> snapshotVersion;

    static ClientLockWatchSnapshot create() {
        return new ClientLockWatchSnapshotImpl();
    }

    private ClientLockWatchSnapshotImpl() {
        this.watches = new HashSet<>();
        this.locked = new HashSet<>();
        this.visitor = new EventVisitor();
        this.snapshotVersion = Optional.empty();
    }

    @Override
    public LockWatchStateUpdate.Snapshot getSnapshot() {
        Preconditions.checkState(snapshotVersion.isPresent(),
                "Snapshot was reset on fail and has not been seeded since");
        return LockWatchStateUpdate.snapshot(
                snapshotVersion.get().id(),
                snapshotVersion.get().version(),
                ImmutableSet.copyOf(locked),
                ImmutableSet.copyOf(watches));
    }

    @Override
    public void processEvents(List<LockWatchEvent> events, IdentifiedVersion lastVersion) {
        assertEventsAreContinguous(events);
        events.forEach(event -> event.accept(visitor));
        snapshotVersion = Optional.of(lastVersion);
    }

    @Override
    public void resetWithSnapshot(LockWatchStateUpdate.Snapshot snapshot) {
        reset();
        watches.addAll(snapshot.lockWatches());
        locked.addAll(snapshot.locked());
        snapshotVersion = Optional.of(IdentifiedVersion.of(snapshot.logId(), snapshot.lastKnownVersion()));
    }

    @Override
    public void reset() {
        snapshotVersion = Optional.empty();
        watches.clear();
        locked.clear();
    }

    private void assertEventsAreContinguous(List<LockWatchEvent> events) {
        if (events.isEmpty()) {
            return;
        }

        if (snapshotVersion.isPresent()) {
            LockWatchEvent firstEvent = Iterables.getFirst(events, null);
            Preconditions.checkNotNull(firstEvent, "First element not preset in list of events");
            Preconditions.checkArgument(snapshotVersion.get().version() + 1 == firstEvent.sequence(),
                    "Events missing between last snapshot and this batch of events");
        }

        for (int i = 0; i < events.size() - 1; ++i) {
            Preconditions.checkArgument(events.get(i).sequence() + 1 == events.get(i + 1).sequence(),
                    "Events form a non-contiguous sequence");
        }
    }

    private final class EventVisitor implements LockWatchEvent.Visitor<Void> {

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
}
