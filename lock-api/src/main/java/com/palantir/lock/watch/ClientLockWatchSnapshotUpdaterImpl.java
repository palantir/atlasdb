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

package com.palantir.lock.watch;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.palantir.lock.LockDescriptor;

public final class ClientLockWatchSnapshotUpdaterImpl implements ClientLockWatchSnapshotUpdater {
    private final Set<LockWatchReferences.LockWatchReference> watches;
    private final Set<LockDescriptor> locked;
    private final EventVisitor visitor;

    public static ClientLockWatchSnapshotUpdater create() {
        return new ClientLockWatchSnapshotUpdaterImpl();
    }

    private ClientLockWatchSnapshotUpdaterImpl() {
        this.watches = new HashSet<>();
        this.locked = new HashSet<>();
        this.visitor = new EventVisitor();
    }

    @Override
    public synchronized LockWatchStateUpdate.Snapshot getSnapshot(IdentifiedVersion identifiedVersion) {
        return LockWatchStateUpdate.snapshot(
                identifiedVersion.id(),
                identifiedVersion.version(),
                ImmutableSet.copyOf(locked),
                ImmutableSet.copyOf(watches));
    }

    @Override
    public synchronized void processEvents(List<LockWatchEvent> events) {
        events.forEach(event -> event.accept(visitor));
    }

    @Override
    public synchronized void resetWithSnapshot(LockWatchStateUpdate.Snapshot snapshot) {
        reset();
        watches.addAll(snapshot.lockWatches());
        locked.addAll(snapshot.locked());
    }

    @Override
    public synchronized void reset() {
        watches.clear();
        locked.clear();
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
