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

import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Streams;
import com.palantir.lock.watch.ClientLockWatchSnapshot;
import com.palantir.lock.watch.ClientLogEvents;
import com.palantir.lock.watch.IdentifiedVersion;
import com.palantir.lock.watch.LockWatchCreatedEvent;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.LockWatchEventLog;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.logsafe.Preconditions;

final class LockWatchEventLogImpl implements LockWatchEventLog {
    private static final boolean INCLUSIVE = true;

    private final ClientLockWatchSnapshot snapshot;
    private final NavigableMap<Long, LockWatchEvent> eventMap = new TreeMap<>();

    private Optional<IdentifiedVersion> latestVersion = Optional.empty();

    static LockWatchEventLog create() {
        return create(ClientLockWatchSnapshotImpl.create());
    }

    @VisibleForTesting
    static LockWatchEventLog create(ClientLockWatchSnapshot snapshot) {
        return new LockWatchEventLogImpl(snapshot);
    }

    private LockWatchEventLogImpl(ClientLockWatchSnapshot snapshot) {
        this.snapshot = snapshot;
    }

    @Override
    public boolean processUpdate(LockWatchStateUpdate update) {
        final ProcessingVisitor visitor;
        if (!latestVersion.isPresent() || !update.logId().equals(latestVersion.get().id())) {
            visitor = new NewLeaderVisitor();
        } else {
            visitor = new ProcessingVisitor();
        }
        return update.accept(visitor);
    }

    @Override
    public ClientLogEvents getEventsBetweenVersions(
            Optional<IdentifiedVersion> startVersion,
            IdentifiedVersion endVersion) {
        Optional<IdentifiedVersion> versionInclusive = startVersion.map(this::createInclusiveVersion);
        IdentifiedVersion currentVersion = getLatestVersionAndVerify(endVersion);
        ClientLogEvents.Builder eventBuilder = new ClientLogEvents.Builder();
        final long fromSequence;

        if (!versionInclusive.isPresent() || differentLeaderOrTooFarBehind(currentVersion, versionInclusive.get())) {
            eventBuilder.addEvents(LockWatchCreatedEvent.fromSnapshot(snapshot.getSnapshot()));
            eventBuilder.clearCache(true);
            if (eventMap.isEmpty()) {
                return eventBuilder.build();
            }
            fromSequence = eventMap.firstKey();
        } else {
            eventBuilder.clearCache(false);
            fromSequence = versionInclusive.get().version();
        }

        eventBuilder.addAllEvents(eventMap.subMap(fromSequence, INCLUSIVE, endVersion.version(), INCLUSIVE).values());
        return eventBuilder.build();
    }

    @Override
    public void removeOldEntries(long earliestSequence) {
        Set<Map.Entry<Long, LockWatchEvent>> eventsToBeRemoved = eventMap.headMap(earliestSequence).entrySet();
        Optional<Long> latestDeletedVersion = Streams.findLast(eventsToBeRemoved.stream()).map(Map.Entry::getKey);
        Optional<IdentifiedVersion> currentVersion = getLatestKnownVersion();

        if (eventsToBeRemoved.isEmpty() || !latestDeletedVersion.isPresent() || !currentVersion.isPresent()) {
            return;
        }

        snapshot.processEvents(
                eventsToBeRemoved.stream().map(Map.Entry::getValue).collect(Collectors.toList()),
                IdentifiedVersion.of(currentVersion.get().id(), latestDeletedVersion.get()));
        eventsToBeRemoved.clear();
    }

    @Override
    public Optional<IdentifiedVersion> getLatestKnownVersion() {
        return latestVersion;
    }

    private boolean differentLeaderOrTooFarBehind(IdentifiedVersion currentVersion,
            IdentifiedVersion startVersion) {
        return !startVersion.id().equals(currentVersion.id()) || eventMap.floorKey(startVersion.version()) == null;
    }

    private IdentifiedVersion createInclusiveVersion(IdentifiedVersion startVersion) {
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

        if (success.lastKnownVersion() > latestVersion.get().version()) {
            success.events().forEach(event -> eventMap.put(event.sequence(), event));
            latestVersion = Optional.of(IdentifiedVersion.of(success.logId(), eventMap.lastKey()));
        }
    }

    private void processSnapshot(LockWatchStateUpdate.Snapshot snapshotUpdate) {
        eventMap.clear();
        this.snapshot.resetWithSnapshot(snapshotUpdate);
        latestVersion = Optional.of(IdentifiedVersion.of(snapshotUpdate.logId(), snapshotUpdate.lastKnownVersion()));
    }

    private void processFailed() {
        eventMap.clear();
        snapshot.reset();
        latestVersion = Optional.empty();
    }

    private class ProcessingVisitor implements LockWatchStateUpdate.Visitor<Boolean> {
        @Override
        public Boolean visit(LockWatchStateUpdate.Failed failed) {
            processFailed();
            return false;
        }

        @Override
        public Boolean visit(LockWatchStateUpdate.Success success) {
            processSuccess(success);
            return true;
        }

        @Override
        public Boolean visit(LockWatchStateUpdate.Snapshot snapshotUpdate) {
            processSnapshot(snapshotUpdate);
            return false;
        }
    }

    private final class NewLeaderVisitor extends ProcessingVisitor {
        @Override
        public Boolean visit(LockWatchStateUpdate.Success success) {
            processFailed();
            return false;
        }
    }

}
