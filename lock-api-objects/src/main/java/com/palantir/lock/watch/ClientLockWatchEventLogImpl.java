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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.palantir.logsafe.Preconditions;

public final class ClientLockWatchEventLogImpl implements ClientLockWatchEventLog {
    private final ClientLockWatchSnapshotUpdater snapshotUpdater;
    private final ConcurrentSkipListMap<IdentifiedVersion, LockWatchEvent> eventLog = new ConcurrentSkipListMap<>();
    private volatile Optional<IdentifiedVersion> latestVersion;

    public ClientLockWatchEventLogImpl() {
        snapshotUpdater = null;
        latestVersion = Optional.empty();
    }

    @Override
    public synchronized TransactionsLockWatchEvents getEventsForTransactions(
            Map<Long, IdentifiedVersion> timestampToVersion,
            Optional<IdentifiedVersion> version) {
        // case 1: their version has no version: tell them to go and get a snapshot.
        // case 2: our version has no version: tell them to get a snapshot, somehow.
        // case 2: their version has a different uuid to yours (leader election): tell them to get a snapshot.
        // case 3: their version is too far behind our log
        if (!version.isPresent()
                || !latestVersion.isPresent()
                || !version.get().id().equals(latestVersion.get().id())
                || eventLog.floorKey(version.get()) == null) {
            return TransactionsLockWatchEvents.failure(snapshotUpdater.getSnapshot());
        }

        IdentifiedVersion mostRecentVersion = Collections.max(timestampToVersion.values());

        Preconditions.checkArgument(mostRecentVersion.compareTo(latestVersion.get()) > 0,
                "Transactions' view of the world is more up-to-date than the log");

        if (eventLog.isEmpty()) {
            return TransactionsLockWatchEvents.success(ImmutableList.of(), timestampToVersion);
        }

        IdentifiedVersion oldestVersion = version.get();

        return TransactionsLockWatchEvents.success(
                new ArrayList<>(eventLog.subMap(oldestVersion, mostRecentVersion).values()),
                timestampToVersion);
    }

    @Override
    public Optional<IdentifiedVersion> getLatestKnownVersion() {
        return latestVersion;
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

    private void processSuccess(
            LockWatchStateUpdate.Success success,
            Optional<IdentifiedVersion> earliestVersion) {
        if (earliestVersion.isPresent()) {
            if (success.lastKnownVersion() >= earliestVersion.get().version()) {
                success.events().forEach(
                        event -> eventLog.put(IdentifiedVersion.of(success.logId(), event.sequence()), event));
                latestVersion = Optional.of(eventLog.lastKey());
            }
            Set<Map.Entry<IdentifiedVersion, LockWatchEvent>> eventsToBeRemoved =
                    eventLog.headMap(earliestVersion.get()).entrySet();
            snapshotUpdater.processEvents(
                    eventsToBeRemoved.stream().map(Map.Entry::getValue).collect(Collectors.toList()));
            eventsToBeRemoved.clear();
        }
    }

    private void processSnapshot(LockWatchStateUpdate.Snapshot snapshot) {
        eventLog.clear();
        snapshotUpdater.resetWithSnapshot(snapshot);
        latestVersion = Optional.of(IdentifiedVersion.of(snapshot.logId(), snapshot.lastKnownVersion()));
    }

    private void processFailed(LockWatchStateUpdate.Failed _failed) {
        eventLog.clear();
        snapshotUpdater.reset();
        latestVersion = Optional.empty();
    }

    private class ProcessingVisitor implements LockWatchStateUpdate.Visitor<Void> {
        private final Optional<IdentifiedVersion> earliestVersion;

        private ProcessingVisitor(Optional<IdentifiedVersion> earliestVersion) {
            this.earliestVersion = earliestVersion;
        }

        @Override
        public Void visit(LockWatchStateUpdate.Failed failed) {
            processFailed(failed);
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

    private class NewLeaderVisitor extends ProcessingVisitor {
        private NewLeaderVisitor(Optional<IdentifiedVersion> earliestVersion) {
            super(earliestVersion);
        }

        @Override
        public Void visit(LockWatchStateUpdate.Success success) {
            processFailed(LockWatchStateUpdate.failed(success.logId()));
            return null;
        }
    }
}
