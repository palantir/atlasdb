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
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.transaction.api.TransactionLockWatchFailedException;
import com.palantir.lock.watch.LockWatchCreatedEvent;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.logsafe.Preconditions;
import java.util.Collection;
import java.util.Optional;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
final class LockWatchEventLog {
    private final ClientLockWatchSnapshot snapshot;
    private final VersionedEventStore eventStore;
    private Optional<LockWatchVersion> latestVersion = Optional.empty();

    static LockWatchEventLog create(int minEvents, int maxEvents) {
        return new LockWatchEventLog(ClientLockWatchSnapshot.create(), minEvents, maxEvents);
    }

    private LockWatchEventLog(ClientLockWatchSnapshot snapshot, int minEvents, int maxEvents) {
        this.snapshot = snapshot;
        this.eventStore = new VersionedEventStore(minEvents, maxEvents);
    }

    CacheUpdate processUpdate(LockWatchStateUpdate update) {
        if (!latestVersion.isPresent()
                || !update.logId().equals(latestVersion.get().id())) {
            return update.accept(new NewLeaderVisitor());
        } else {
            return update.accept(new ProcessingVisitor());
        }
    }

    /**
     * @return lock watch events that occurred from (exclusive) the provided version, up to the end version (inclusive);
     *         this may begin with a snapshot if the latest version is too far behind, and this snapshot may be
     *         condensed.
     */
    ClientLogEvents getEventsBetweenVersions(VersionBounds versionBounds) {
        Optional<LockWatchVersion> startVersion = versionBounds.startVersion().map(this::createStartVersion);
        LockWatchVersion currentVersion = getLatestVersionAndVerify(versionBounds.endVersion());

        if (!startVersion.isPresent()
                || differentLeaderOrTooFarBehind(
                        currentVersion, versionBounds.startVersion().get(), startVersion.get())) {
            long snapshotVersion = versionBounds.snapshotVersion() + 1;
            Collection<LockWatchEvent> afterSnapshotEvents;
            if (snapshotVersion > versionBounds.endVersion().version()) {
                afterSnapshotEvents = ImmutableList.of();
            } else {
                afterSnapshotEvents = eventStore.getEventsBetweenVersionsInclusive(
                        Optional.of(snapshotVersion), versionBounds.endVersion().version());
            }
            return new ClientLogEvents.Builder()
                    .clearCache(true)
                    .events(LockWatchEvents.builder()
                            .addEvents(getCompressedSnapshot(versionBounds))
                            .addAllEvents(afterSnapshotEvents)
                            .build())
                    .build();
        } else {
            versionBounds
                    .startVersion()
                    .ifPresent(version -> Preconditions.checkState(
                            version.version() <= versionBounds.endVersion().version(),
                            "Cannot get update for transactions when the last known version is more recent than the "
                                    + "transactions"));
            return new ClientLogEvents.Builder()
                    .clearCache(false)
                    .events(LockWatchEvents.builder()
                            .addAllEvents(eventStore.getEventsBetweenVersionsInclusive(
                                    Optional.of(startVersion.get().version()),
                                    versionBounds.endVersion().version()))
                            .build())
                    .build();
        }
    }

    void retentionEvents(Optional<Sequence> earliestSequence) {
        getLatestKnownVersion().ifPresent(version -> {
            LockWatchEvents eventsToBeRemoved = eventStore.retentionEvents(earliestSequence);
            snapshot.processEvents(eventsToBeRemoved, version.id());
        });
    }

    Optional<LockWatchVersion> getLatestKnownVersion() {
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

    private LockWatchEvent getCompressedSnapshot(VersionBounds versionBounds) {
        long snapshotVersion = versionBounds.snapshotVersion();
        Collection<LockWatchEvent> collapsibleEvents =
                eventStore.getEventsBetweenVersionsInclusive(Optional.empty(), snapshotVersion);
        LockWatchEvents events =
                LockWatchEvents.builder().addAllEvents(collapsibleEvents).build();

        return LockWatchCreatedEvent.fromSnapshot(snapshot.getSnapshotWithEvents(events, versionBounds.leader()));
    }

    private boolean differentLeaderOrTooFarBehind(
            LockWatchVersion currentVersion, LockWatchVersion lastKnownVersion, LockWatchVersion startVersion) {
        if (!startVersion.id().equals(currentVersion.id())) {
            return true;
        }
        if (latestVersion.filter(lastKnownVersion::equals).isPresent()) {
            return false;
        }
        return !eventStore.containsEntryLessThanOrEqualTo(startVersion.version());
    }

    private LockWatchVersion createStartVersion(LockWatchVersion startVersion) {
        return LockWatchVersion.of(startVersion.id(), startVersion.version() + 1);
    }

    private LockWatchVersion getLatestVersionAndVerify(LockWatchVersion endVersion) {
        Preconditions.checkState(latestVersion.isPresent(), "Cannot get events when log does not know its version");
        LockWatchVersion currentVersion = latestVersion.get();
        Preconditions.checkArgument(
                endVersion.version() <= currentVersion.version(),
                "Transactions' view of the world is more up-to-date than the log");
        return currentVersion;
    }

    private void processSuccess(LockWatchStateUpdate.Success success) {
        Preconditions.checkState(latestVersion.isPresent(), "Must have a known version to process successful updates");
        Optional<LockWatchVersion> snapshotVersion = snapshot.getSnapshotVersion();
        Preconditions.checkState(
                snapshotVersion.isPresent(), "Must have a snapshot before processing successful updates");

        if (success.lastKnownVersion() < snapshotVersion.get().version()) {
            throw new TransactionLockWatchFailedException(
                    "Cannot process events before the oldest event. The transaction should be retried, although this"
                            + " should only happen very rarely.");
        }

        if (success.lastKnownVersion() > latestVersion.get().version()) {
            LockWatchEvents events =
                    LockWatchEvents.builder().events(success.events()).build();
            if (events.events().isEmpty()) {
                throw new TransactionLockWatchFailedException("Success event has a later version than the current "
                        + "version, but has no events to bridge the gap. The transaction should be tried, but this "
                        + "should only happen rarely.");
            }

            events.assertNoEventsAreMissingAfterLatestVersion(latestVersion);
            latestVersion = Optional.of(LockWatchVersion.of(success.logId(), eventStore.putAll(events)));
        }
    }

    private void processSnapshot(LockWatchStateUpdate.Snapshot snapshotUpdate) {
        eventStore.clear();
        snapshot.resetWithSnapshot(snapshotUpdate);
        latestVersion = Optional.of(LockWatchVersion.of(snapshotUpdate.logId(), snapshotUpdate.lastKnownVersion()));
    }

    private void processFailed() {
        eventStore.clear();
        snapshot.reset();
        latestVersion = Optional.empty();
    }

    private final class ProcessingVisitor implements LockWatchStateUpdate.Visitor<CacheUpdate> {

        @Override
        public CacheUpdate visit(LockWatchStateUpdate.Success success) {
            processSuccess(success);
            return new CacheUpdate(
                    false, Optional.of(LockWatchVersion.of(success.logId(), success.lastKnownVersion())));
        }

        @Override
        public CacheUpdate visit(LockWatchStateUpdate.Snapshot snapshotUpdate) {
            processSnapshot(snapshotUpdate);
            return new CacheUpdate(true, latestVersion);
        }
    }

    private final class NewLeaderVisitor implements LockWatchStateUpdate.Visitor<CacheUpdate> {

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
