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

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.concurrent.GuardedBy;
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
        ensureNotFailed(() -> {
            final ProcessingVisitor visitor;
            if (!latestVersion.isPresent() || !update.logId().equals(latestVersion.get().id())) {
                visitor = new NewLeaderVisitor();
            } else {
                visitor = new ProcessingVisitor();
            }
            update.accept(visitor);
        });
        return latestVersion;
    }

    @Override
    public synchronized void removeOldEntries(IdentifiedVersion earliestVersion) {
        ensureNotFailed(() -> {
            Set<Map.Entry<Long, LockWatchEvent>> eventsToBeRemoved =
                    eventMap.headMap(earliestVersion.version()).entrySet();
            snapshotUpdater.processEvents(
                    eventsToBeRemoved.stream().map(Map.Entry::getValue).collect(Collectors.toList()));
            eventsToBeRemoved.clear();
        });
    }

    /**
     * @param startVersion latest version that the client knows about; should be before timestamps in the mapping;
     * @param endVersion   mapping from timestamp to identified version from client-side event cache;
     * @return lock watch events that occurred from (exclusive) the provided version, up to (inclusive) the latest
     * version in the timestamp to version map.
     */
    @Override
    public synchronized ClientEventUpdate getEventsBetweenVersions(
            Optional<IdentifiedVersion> startVersion,
            IdentifiedVersion endVersion) {
        checkNotFailed();
        Optional<IdentifiedVersion> versionInclusive = startVersion.map(this::createInclusiveVersion);
        IdentifiedVersion currentVersion = getLatestVersionAndVerify(endVersion);

        if (!versionInclusive.isPresent() || differentLeaderOrTooFarBehind(currentVersion, versionInclusive.get())) {
            return ClientEventUpdate.failure(snapshotUpdater.getSnapshot(currentVersion));
        }

        if (eventMap.isEmpty()) {
            return ClientEventUpdate.success(ImmutableList.of());
        }

        IdentifiedVersion fromVersion = versionInclusive.get();

        return ClientEventUpdate.success(ImmutableList.copyOf(
                eventMap.subMap(fromVersion.version(), INCLUSIVE, endVersion.version(), INCLUSIVE).values()));
    }

    @Override
    public synchronized Optional<IdentifiedVersion> getLatestKnownVersion() {
        checkNotFailed();
        return latestVersion;
    }

    private void ensureNotFailed(Runnable runnable) {
        checkNotFailed();
        failed = true;
        runnable.run();
        failed = false;
    }

    private void checkNotFailed() {
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

}
