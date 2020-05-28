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
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.lock.LockDescriptor;
import com.palantir.logsafe.Preconditions;

public final class ClientLockWatchEventLogImpl implements ClientLockWatchEventLog {
    private final ClientLockWatchSnapshotUpdater snapshotUpdater;
    private final ConcurrentSkipListMap<Long, LockWatchEvent> eventMap = new ConcurrentSkipListMap<>();
    private volatile Optional<IdentifiedVersion> latestVersion = Optional.empty();

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
    public synchronized Optional<IdentifiedVersion> processUpdate(
            LockWatchStateUpdate update,
            Optional<IdentifiedVersion> earliestVersion) {
        ProcessingVisitor visitor;
        if (!latestVersion.isPresent() || !update.logId().equals(latestVersion.get().id())) {
            visitor = new NewLeaderVisitor(earliestVersion);
        } else {
            visitor = new ProcessingVisitor(earliestVersion);
        }
        update.accept(visitor);
        return latestVersion;
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
        Optional<IdentifiedVersion> versionInclusive = version.map(this::getInclusiveVersion);
        IdentifiedVersion toVersion = Collections.max(timestampToVersion.values());
        IdentifiedVersion currentVersion = getLatestVersionAndVerify(toVersion);

        /*
        There are three cases to consider where we would return a snapshot:
            1: they provide an empty version;
            2. their version has a different UUID (i.e. refers to the wrong leader);
            3. their version is behind our log.
        Note that if their version is ahead of our log, or we do not have a version, an exception is thrown instead.
         */
        if (!versionInclusive.isPresent()
                || !versionInclusive.get().id().equals(currentVersion.id())
                || eventMap.floorKey(versionInclusive.get().version()) == null) {
            return TransactionsLockWatchEvents.failure(snapshotUpdater.getSnapshot(currentVersion));
        }

        if (eventMap.isEmpty()) {
            return TransactionsLockWatchEvents.success(ImmutableList.of(), timestampToVersion);
        }

        IdentifiedVersion fromVersion = versionInclusive.get();

        return TransactionsLockWatchEvents.success(
                new ArrayList<>(eventMap.subMap(fromVersion.version(), true, toVersion.version(), true).values()),
                timestampToVersion);
    }

    @Override
    public synchronized Optional<Set<LockDescriptor>> getEventsBetweenVersions(
            IdentifiedVersion startVersion,
            IdentifiedVersion endVersion) {
        IdentifiedVersion currentVersion = getLatestVersionAndVerify(endVersion);

        if (eventMap.isEmpty()) {
            return Optional.of(ImmutableSet.of());
        }

        IdentifiedVersion fromVersion = getInclusiveVersion(startVersion);

        if (!fromVersion.id().equals(currentVersion.id())
                || eventMap.floorKey(fromVersion.version()) == null) {
            return Optional.empty();
        }

        List<LockWatchEvent> events =
                new ArrayList<>(eventMap.subMap(fromVersion.version(), true, endVersion.version(), true).values());
        Set<LockDescriptor> locksTakenOut = new HashSet<>();
        events.forEach(event -> locksTakenOut.addAll(event.accept(LockEventVisitor.INSTANCE)));

        return Optional.of(locksTakenOut);
    }

    @Override
    public Optional<IdentifiedVersion> getLatestKnownVersion() {
        return latestVersion;
    }

    private IdentifiedVersion getInclusiveVersion(IdentifiedVersion startVersion) {
        return IdentifiedVersion.of(startVersion.id(), startVersion.version() + 1);
    }

    private IdentifiedVersion getLatestVersionAndVerify(IdentifiedVersion endVersion) {
        Preconditions.checkState(latestVersion.isPresent(), "Cannot get events when log does not know its version");
        IdentifiedVersion currentVersion = latestVersion.get();
        Preconditions.checkArgument(endVersion.compareTo(currentVersion) > -1,
                "Transactions' view of the world is more up-to-date than the log");
        return currentVersion;
    }

    private void processSuccess(
            LockWatchStateUpdate.Success success,
            Optional<IdentifiedVersion> earliestLivingVersion) {
        Preconditions.checkState(latestVersion.isPresent(), "Must have a known version to process successful updates");

        // Avoid processing old entries
        if (success.lastKnownVersion() > latestVersion.get().version()) {
            success.events().forEach(event ->
                    eventMap.put(event.sequence(), event));
            latestVersion = Optional.of(IdentifiedVersion.of(success.logId(), eventMap.lastKey()));
        }

        // Remove old entries, up to (exclusive) the earliest living version
        if (earliestLivingVersion.isPresent()) {
            Set<Map.Entry<Long, LockWatchEvent>> eventsToBeRemoved =
                    eventMap.headMap(earliestLivingVersion.get().version()).entrySet();
            snapshotUpdater.processEvents(
                    eventsToBeRemoved.stream().map(Map.Entry::getValue).collect(Collectors.toList()));
            eventsToBeRemoved.clear();
        }
    }

    private void processSnapshot(LockWatchStateUpdate.Snapshot snapshot) {
        eventMap.clear();
        snapshotUpdater.resetWithSnapshot(snapshot);
        latestVersion = Optional.of(IdentifiedVersion.of(snapshot.logId(), snapshot.lastKnownVersion()));
    }

    private void processFailed() {
        eventMap.clear();
        snapshotUpdater.reset();
        latestVersion = Optional.empty();
    }

    private class ProcessingVisitor implements LockWatchStateUpdate.Visitor<Void> {
        private final Optional<IdentifiedVersion> earliestVersion;

        private ProcessingVisitor(Optional<IdentifiedVersion> earliestVersion) {
            this.earliestVersion = earliestVersion;
        }

        @Override
        public Void visit(LockWatchStateUpdate.Failed _failed) {
            processFailed();
            return null;
        }

        @Override
        public Void visit(LockWatchStateUpdate.Success success) {
            processSuccess(success, earliestVersion);
            return null;
        }

        @Override
        public Void visit(LockWatchStateUpdate.Snapshot snapshot) {
            processSnapshot(snapshot);
            return null;
        }
    }

    private final class NewLeaderVisitor extends ProcessingVisitor {
        private NewLeaderVisitor(Optional<IdentifiedVersion> earliestVersion) {
            super(earliestVersion);
        }

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
