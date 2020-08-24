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

import java.util.List;
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.transaction.api.TransactionLockWatchFailedException;
import com.palantir.lock.watch.IdentifiedVersion;
import com.palantir.lock.watch.LockWatchCreatedEvent;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.logsafe.Preconditions;

final class LockWatchEventLog {
    private final ClientLockWatchSnapshot snapshot;
    private final VersionedEventStore eventStore = new VersionedEventStore();
    private Optional<IdentifiedVersion> latestVersion = Optional.empty();

    static LockWatchEventLog create() {
        return create(ClientLockWatchSnapshot.create());
    }

    @VisibleForTesting
    static LockWatchEventLog create(ClientLockWatchSnapshot snapshot) {
        return new LockWatchEventLog(snapshot);
    }

    private LockWatchEventLog(ClientLockWatchSnapshot snapshot) {
        this.snapshot = snapshot;
    }

    CacheUpdate processUpdate(LockWatchStateUpdate update) {
        if (!latestVersion.isPresent() || !update.logId().equals(latestVersion.get().id())) {
            return update.accept(new NewLeaderVisitor());
        } else {
            return update.accept(new ProcessingVisitor());
        }
    }

    /**
     * @param lastKnownVersion latest version that the client knows about; should be before timestamps in the mapping;
     * @param endVersion       mapping from timestamp to identified version from client-side event cache;
     * @return lock watch events that occurred from (exclusive) the provided version, up to (inclusive) the latest
     * version in the timestamp to version map.
     */
    public ClientLogEvents getEventsBetweenVersions(
            Optional<IdentifiedVersion> lastKnownVersion,
            IdentifiedVersion endVersion) {
        Optional<IdentifiedVersion> startVersion = lastKnownVersion.map(this::createStartVersion);
        IdentifiedVersion currentVersion = getLatestVersionAndVerify(endVersion);

        if (!startVersion.isPresent() || differentLeaderOrTooFarBehind(currentVersion, startVersion.get())) {
            return new ClientLogEvents.Builder()
                    .clearCache(true)
                    .addEvents(LockWatchCreatedEvent.fromSnapshot(snapshot.getSnapshot()))
                    .addAllEvents(eventStore.getEventsBetweenVersionsInclusive(Optional.empty(), endVersion.version()))
                    .build();
        } else {
            return new ClientLogEvents.Builder()
                    .clearCache(false)
                    .addAllEvents(
                            eventStore.getEventsBetweenVersionsInclusive(Optional.of(startVersion.get().version()),
                                    endVersion.version()))
                    .build();
        }
    }

    void removeEventsBefore(long earliestSequence) {
        getLatestKnownVersion().ifPresent(version -> {
            LockWatchEvents eventsToBeRemoved = eventStore.getAndRemoveElementsUpToExclusive(earliestSequence);
            snapshot.processEvents(eventsToBeRemoved, version.id());
        });
    }

    Optional<IdentifiedVersion> getLatestKnownVersion() {
        return latestVersion;
    }

    @VisibleForTesting
    LockWatchEventLogState getStateForTesting() {
        return ImmutableLockWatchEventLogState.builder()
                .latestVersion(latestVersion)
                .eventStoreState(eventStore.getStateForTesting())
                .snapshotState(snapshot.getStateForTesting())
                .build();
    }

    private boolean differentLeaderOrTooFarBehind(IdentifiedVersion currentVersion,
            IdentifiedVersion startVersion) {
        return !startVersion.id().equals(currentVersion.id()) || !eventStore.contains(startVersion.version());
    }

    private IdentifiedVersion createStartVersion(IdentifiedVersion startVersion) {
        return IdentifiedVersion.of(startVersion.id(), startVersion.version() + 1);
    }

    private IdentifiedVersion getLatestVersionAndVerify(IdentifiedVersion endVersion) {
        Preconditions.checkState(latestVersion.isPresent(), "Cannot get events when log does not know its version");
        IdentifiedVersion currentVersion = latestVersion.get();
        Preconditions.checkArgument(endVersion.version() <= currentVersion.version(),
                "Transactions' view of the world is more up-to-date than the log");
        return currentVersion;
    }

    private void processSuccess(LockWatchStateUpdate.Success success) {
        Preconditions.checkState(latestVersion.isPresent(), "Must have a known version to process successful updates");
        Optional<IdentifiedVersion> snapshotVersion = snapshot.getSnapshotVersion();
        Preconditions.checkState(snapshotVersion.isPresent(),
                "Must have a snapshot before processing successful updates");

        if (success.lastKnownVersion() < snapshotVersion.get().version()) {
            throw new TransactionLockWatchFailedException(
                    "Cannot process events before the oldest event. The transaction should be retried, although this"
                            + " should only happen very rarely.");
        }

        if (success.lastKnownVersion() > latestVersion.get().version()) {
            assertEventsAreContiguousAndNoEventsMissing(success.events());
            latestVersion = Optional.of(IdentifiedVersion.of(success.logId(), eventStore.putAll(success.events())));
        }
    }

    private void assertEventsAreContiguousAndNoEventsMissing(List<LockWatchEvent> events) {
        if (events.isEmpty()) {
            return;
        }

        for (int i = 0; i < events.size() - 1; ++i) {
            Preconditions.checkArgument(events.get(i).sequence() + 1 == events.get(i + 1).sequence(),
                    "Events form a non-contiguous sequence");
        }

        if (latestVersion.isPresent()) {
            LockWatchEvent firstEvent = Iterables.getFirst(events, null);
            Preconditions.checkNotNull(firstEvent, "First element not preset in list of events");
            Preconditions.checkArgument(firstEvent.sequence() <= latestVersion.get().version()
                            || latestVersion.get().version() + 1 == firstEvent.sequence(),
                    "Events missing between last snapshot and this batch of events");
        }
    }

    private void processSnapshot(LockWatchStateUpdate.Snapshot snapshotUpdate) {
        eventStore.clear();
        snapshot.resetWithSnapshot(snapshotUpdate);
        latestVersion = Optional.of(IdentifiedVersion.of(snapshotUpdate.logId(), snapshotUpdate.lastKnownVersion()));
    }

    private void processFailed() {
        eventStore.clear();
        snapshot.reset();
        latestVersion = Optional.empty();
    }

    private class ProcessingVisitor implements LockWatchStateUpdate.Visitor<CacheUpdate> {

        @Override
        public CacheUpdate visit(LockWatchStateUpdate.Success success) {
            processSuccess(success);
            return new CacheUpdate(false,
                    Optional.of(IdentifiedVersion.of(success.logId(), success.lastKnownVersion())));
        }

        @Override
        public CacheUpdate visit(LockWatchStateUpdate.Snapshot snapshotUpdate) {
            processSnapshot(snapshotUpdate);
            return new CacheUpdate(true, latestVersion);
        }
    }

    private class NewLeaderVisitor implements LockWatchStateUpdate.Visitor<CacheUpdate> {

        @Override
        public CacheUpdate visit(LockWatchStateUpdate.Success success) {
            processFailed();
            return CacheUpdate.FAILED;
        }

        @Override
        public CacheUpdate visit(LockWatchStateUpdate.Snapshot snapshotUpdate) {
            processSnapshot(snapshotUpdate);
            return new CacheUpdate(true, latestVersion);
        }
    }
}
