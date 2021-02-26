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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.watch.LockEvent;
import com.palantir.lock.watch.LockWatchCreatedEvent;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.LockWatchReferences;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.lock.watch.UnlockEvent;
import com.palantir.logsafe.Preconditions;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

final class ClientLockWatchSnapshot {
    private final Set<LockWatchReferences.LockWatchReference> watches;
    private final Set<LockDescriptor> locked;
    private final EventVisitor visitor;
    private Optional<LockWatchVersion> snapshotVersion;

    static ClientLockWatchSnapshot create() {
        return new ClientLockWatchSnapshot();
    }

    private ClientLockWatchSnapshot() {
        this.watches = new HashSet<>();
        this.locked = new HashSet<>();
        this.visitor = new EventVisitor();
        this.snapshotVersion = Optional.empty();
    }

    private LockWatchStateUpdate.Snapshot getSnapshot() {
        Preconditions.checkState(
                snapshotVersion.isPresent(), "Snapshot was reset on fail and has not been seeded since");
        return LockWatchStateUpdate.snapshot(
                snapshotVersion.get().id(),
                snapshotVersion.get().version(),
                ImmutableSet.copyOf(locked),
                ImmutableSet.copyOf(watches));
    }

    LockWatchStateUpdate.Snapshot getSnapshotWithEvents(LockWatchEvents events, UUID versionId) {
        ClientLockWatchSnapshot freshSnapshot = create();
        freshSnapshot.resetWithSnapshot(getSnapshot());
        freshSnapshot.processEvents(events, versionId);
        return freshSnapshot.getSnapshot();
    }

    void processEvents(LockWatchEvents events, UUID versionId) {
        if (events.events().isEmpty()) {
            return;
        }

        events.assertNoEventsAreMissingAfterLatestVersion(snapshotVersion);
        events.events().forEach(event -> event.accept(visitor));
        snapshotVersion = Optional.of(LockWatchVersion.of(
                versionId, events.versionRange().map(Range::upperEndpoint).get()));
    }

    void resetWithSnapshot(LockWatchStateUpdate.Snapshot snapshot) {
        reset();
        watches.addAll(snapshot.lockWatches());
        locked.addAll(snapshot.locked());
        snapshotVersion = Optional.of(LockWatchVersion.of(snapshot.logId(), snapshot.lastKnownVersion()));
    }

    void reset() {
        snapshotVersion = Optional.empty();
        watches.clear();
        locked.clear();
    }

    Optional<LockWatchVersion> getSnapshotVersion() {
        return snapshotVersion;
    }

    @VisibleForTesting
    ClientLockWatchSnapshotState getStateForTesting() {
        return ImmutableClientLockWatchSnapshotState.builder()
                .snapshotVersion(snapshotVersion)
                .locked(locked)
                .watches(watches)
                .build();
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
