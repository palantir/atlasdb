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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import javax.annotation.concurrent.GuardedBy;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.lock.LockDescriptor;
import com.palantir.logsafe.Preconditions;

public final class ClientLockWatchEventLogImpl implements ClientLockWatchEventLog {
    private static final boolean INCLUSIVE = true;

    @GuardedBy("this")
    private final ClientLockWatchSnapshotUpdater snapshotUpdater;

    @GuardedBy("this")
    private final TreeMap<Long, LockWatchEvent> eventMap = new TreeMap<>();

    @GuardedBy("this")
    private boolean failed = false;

    @GuardedBy("this")
    private Optional<IdentifiedVersion> latestVersion = Optional.empty();

    public static ClientLockWatchEventLogImpl create() {
        return create(ClientLockWatchSnapshotUpdaterImpl.create());
    }

    @VisibleForTesting
    static ClientLockWatchEventLogImpl create(ClientLockWatchSnapshotUpdater snapshotUpdater) {
        return new ClientLockWatchEventLogImpl(snapshotUpdater);
    }

    private ClientLockWatchEventLogImpl(ClientLockWatchSnapshotUpdater snapshotUpdater) {
        this.snapshotUpdater = snapshotUpdater;
    }

    @Override
    public synchronized Optional<IdentifiedVersion> processUpdate(LockWatchStateUpdate update) {
        checkFailed();
        failed = true;
        final ProcessingVisitor visitor;
        if (!latestVersion.isPresent() || !update.logId().equals(latestVersion.get().id())) {
            visitor = new NewLeaderVisitor();
        } else {
            visitor = new ProcessingVisitor();
        }
        update.accept(visitor);
        failed = false;
        return latestVersion;
    }

    @Override
    public synchronized void removeOldEntries(IdentifiedVersion earliestVersion) {
        checkFailed();
        failed = true;
        Set<Map.Entry<Long, LockWatchEvent>> eventsToBeRemoved =
                eventMap.headMap(earliestVersion.version()).entrySet();
        snapshotUpdater.processEvents(
                eventsToBeRemoved.stream().map(Map.Entry::getValue).collect(Collectors.toList()));
        eventsToBeRemoved.clear();
        failed = false;
    }

    /**
     * @param timestampToVersion mapping from timestamp to identified version from client-side event cache;
     * @param version            latest version that the client knows about; should be before timestamps in the
     *                           mapping;
     * @return lock watch events that occurred from (exclusive) the provided version, up to (inclusive) the latest
     * version in the timestamp to version map.
     */
    @Override
    public synchronized TransactionsLockWatchEvents getEventsForTransactions(
            Map<Long, IdentifiedVersion> timestampToVersion,
            Optional<IdentifiedVersion> version) {
        checkFailed();
        Optional<IdentifiedVersion> versionInclusive = version.map(this::createInclusiveVersion);
        IdentifiedVersion toVersion = Collections.max(timestampToVersion.values(), IdentifiedVersion.comparator());
        IdentifiedVersion currentVersion = getLatestVersionAndVerify(toVersion);

        if (!versionInclusive.isPresent() || differentLeaderOrTooFarBehind(currentVersion, versionInclusive.get())) {
            return TransactionsLockWatchEvents.failure(snapshotUpdater.getSnapshot(currentVersion));
        }

        if (eventMap.isEmpty()) {
            return TransactionsLockWatchEvents.success(ImmutableList.of(), timestampToVersion);
        }

        IdentifiedVersion fromVersion = versionInclusive.get();

        return TransactionsLockWatchEvents.success(
                ImmutableList.copyOf(
                        eventMap.subMap(fromVersion.version(), INCLUSIVE, toVersion.version(), INCLUSIVE).values()),
                timestampToVersion);
    }

    @Override
    public synchronized Optional<Set<LockDescriptor>> getEventsBetweenVersions(
            IdentifiedVersion startVersion,
            IdentifiedVersion endVersion) {
        IdentifiedVersion currentVersion = getLatestVersionAndVerify(endVersion);
        IdentifiedVersion fromVersion = createInclusiveVersion(startVersion);

        if (differentLeaderOrTooFarBehind(currentVersion, fromVersion)) {
            return Optional.empty();
        }

        if (eventMap.isEmpty()) {
            return Optional.of(ImmutableSet.of());
        }

        List<LockWatchEvent> events =
                new ArrayList<>(eventMap.subMap(fromVersion.version(), true, endVersion.version(), true).values());
        Set<LockDescriptor> locksTakenOut = new HashSet<>();
        events.forEach(event -> locksTakenOut.addAll(event.accept(LockEventVisitor.INSTANCE)));

        return Optional.of(locksTakenOut);
    }

    @Override
    public synchronized Optional<IdentifiedVersion> getLatestKnownVersion() {
        return latestVersion;
    }

    private void checkFailed() {
        Preconditions.checkState(!failed, "Log is in an inconsistent state");
    }

    private boolean differentLeaderOrTooFarBehind(IdentifiedVersion currentVersion, IdentifiedVersion startVersion) {
        return !startVersion.id().equals(currentVersion.id()) || eventMap.floorKey(startVersion.version()) == null;
    }

    private IdentifiedVersion createInclusiveVersion(IdentifiedVersion startVersion) {
        return IdentifiedVersion.of(startVersion.id(), startVersion.version() + 1);
    }

    private IdentifiedVersion getLatestVersionAndVerify(IdentifiedVersion endVersion) {
        Preconditions.checkState(latestVersion.isPresent(), "Cannot get events when log does not know its version");
        IdentifiedVersion currentVersion = latestVersion.get();
        Preconditions.checkArgument(IdentifiedVersion.comparator().compare(endVersion, currentVersion) > -1,
                "Transactions' view of the world is more up-to-date than the log");
        return currentVersion;
    }

    private synchronized void processSuccess(LockWatchStateUpdate.Success success) {
        Preconditions.checkState(latestVersion.isPresent(), "Must have a known version to process successful updates");

        if (success.lastKnownVersion() > latestVersion.get().version()) {
            success.events().forEach(event ->
                    eventMap.put(event.sequence(), event));
            latestVersion = Optional.of(IdentifiedVersion.of(success.logId(), eventMap.lastKey()));
        }
    }

    private synchronized void processSnapshot(LockWatchStateUpdate.Snapshot snapshot) {
        eventMap.clear();
        snapshotUpdater.resetWithSnapshot(snapshot);
        latestVersion = Optional.of(IdentifiedVersion.of(snapshot.logId(), snapshot.lastKnownVersion()));
    }

    private synchronized void processFailed() {
        eventMap.clear();
        snapshotUpdater.reset();
        latestVersion = Optional.empty();
    }

    private class ProcessingVisitor implements LockWatchStateUpdate.Visitor<Void> {
        @Override
        public Void visit(LockWatchStateUpdate.Failed _failed) {
            processFailed();
            return null;
        }

        @Override
        public Void visit(LockWatchStateUpdate.Success success) {
            processSuccess(success);
            return null;
        }

        @Override
        public Void visit(LockWatchStateUpdate.Snapshot snapshot) {
            processSnapshot(snapshot);
            return null;
        }
    }

    private final class NewLeaderVisitor extends ProcessingVisitor {
        @Override
        public Void visit(LockWatchStateUpdate.Success _success) {
            processFailed();
            return null;
        }
    }

    enum LockEventVisitor implements LockWatchEvent.Visitor<Set<LockDescriptor>> {
        INSTANCE;

        @Override
        public Set<LockDescriptor> visit(LockEvent lockEvent) {
            return lockEvent.lockDescriptors();
        }

        @Override
        public Set<LockDescriptor> visit(UnlockEvent unlockEvent) {
            return ImmutableSet.of();
        }

        @Override
        public Set<LockDescriptor> visit(LockWatchCreatedEvent lockWatchCreatedEvent) {
            return lockWatchCreatedEvent.lockDescriptors();
        }
    }
}
