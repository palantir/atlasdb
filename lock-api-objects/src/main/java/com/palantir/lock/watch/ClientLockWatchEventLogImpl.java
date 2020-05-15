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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

public final class ClientLockWatchEventLogImpl implements ClientLockWatchEventLog {
    private final ProcessingVisitor processingVisitor = new ProcessingVisitor();
    private final NewLeaderVisitor newLeaderVisitor = new NewLeaderVisitor();
    private volatile ConcurrentSkipListMap<Long, LockWatchEvent> eventLog;
    private final ClientLockWatchSnapshotUpdater snapshotUpdater;
    private final int maxSize = 2000;
    private volatile IdentifiedVersion identifiedVersion;

    public ClientLockWatchEventLogImpl() {
        identifiedVersion = IdentifiedVersion.of(UUID.randomUUID(), Optional.empty());
        eventLog = new ConcurrentSkipListMap<>();
        snapshotUpdater = null; // todo - implement
    }

    @Override
    public IdentifiedVersion getLatestKnownVersion() {
        return identifiedVersion;
    }

    @Override
    public IdentifiedVersion processUpdate(LockWatchStateUpdate update) {
        if (update.logId().equals(identifiedVersion.id())) {
            return update.accept(processingVisitor);
        } else {
            return update.accept(newLeaderVisitor);
        }
    }

    @Override
    public TransactionsLockWatchEvents getEventsForTransactions(
            Map<Long, Long> timestampToVersion,
            IdentifiedVersion version) {
        IdentifiedVersion currentVersion = identifiedVersion;
        // case 1: their version has no version: tell them to go and get a snapshot.
        // case 2: their version has a different uuid to yours (leader election): tell them to get a snapshot.
        // case 3: their version is too far behind our log
        if (!version.version().isPresent()
                || !version.id().equals(currentVersion.id())
                || eventLog.floorKey(version.version().get()) == null) {
            return TransactionsLockWatchEvents.failure(snapshotUpdater.getSnapshot());
        }

        // otherwise, we assume that actually you are in a good state.

        long timestampLatestVersion = Collections.max(timestampToVersion.values());
        ConcurrentSkipListMap<Long, LockWatchEvent> logSnapshot = eventLog.clone();

        if (logSnapshot.isEmpty()) {
            return TransactionsLockWatchEvents.success(ImmutableList.of(), timestampToVersion);
        }

        long oldestVersion = version.version().get();

        long startKey = logSnapshot.ceilingKey(oldestVersion);
        long endKey = logSnapshot.floorKey(timestampLatestVersion);

        return TransactionsLockWatchEvents.success(
                new ArrayList<>(logSnapshot.subMap(startKey, endKey).values()),
                timestampToVersion);
    }


    private synchronized IdentifiedVersion processSnapshot(LockWatchStateUpdate.Snapshot snapshot) {
        IdentifiedVersion newVersion = IdentifiedVersion.of(snapshot.logId(), Optional.of(snapshot.lastKnownVersion()));
        identifiedVersion = newVersion;
        eventLog = new ConcurrentSkipListMap<>();
        snapshotUpdater.resetWithSnapshot(snapshot);
        return newVersion;
    }

    private synchronized IdentifiedVersion processFailed(LockWatchStateUpdate.Failed failed) {
        IdentifiedVersion newVersion = IdentifiedVersion.of(failed.logId(), Optional.empty());
        identifiedVersion = newVersion;
        eventLog = new ConcurrentSkipListMap<>();
        snapshotUpdater.reset();
        return newVersion;
    }

    private IdentifiedVersion processSuccess(LockWatchStateUpdate.Success success) {
        // case where this update is from ages ago, and so has already been deleted.
        if (success.lastKnownVersion() < eventLog.firstKey()) {
            return identifiedVersion;
        }
        success.events().forEach(event -> eventLog.put(event.sequence(), event));
        IdentifiedVersion newVersion = IdentifiedVersion.of(success.logId(), Optional.of(eventLog.lastKey()));
        identifiedVersion = newVersion;
        evictIfFull();
        return newVersion;
    }

    private void evictIfFull() {
        int excess = eventLog.size() - maxSize;
        if (excess > 0) {
            evict(excess);
        }
    }

    private void evict(int count) {
        List<Map.Entry<Long, LockWatchEvent>> entriesToEvict = eventLog.entrySet().stream().limit(count).collect(
                Collectors.toList());

        snapshotUpdater.processEvents(entriesToEvict.stream().map(Map.Entry::getValue).collect(Collectors.toList()));
        entriesToEvict.forEach(entry -> eventLog.remove(entry.getKey()));
    }

    private class ProcessingVisitor implements LockWatchStateUpdate.Visitor<IdentifiedVersion> {
        @Override
        public IdentifiedVersion visit(LockWatchStateUpdate.Failed failed) {
            return processFailed(failed);
        }

        @Override
        public IdentifiedVersion visit(LockWatchStateUpdate.Success success) {
            return processSuccess(success);
        }

        @Override
        public IdentifiedVersion visit(LockWatchStateUpdate.Snapshot snapshot) {
            return processSnapshot(snapshot);
        }
    }

    private class NewLeaderVisitor extends ProcessingVisitor {
        @Override
        public IdentifiedVersion visit(LockWatchStateUpdate.Success success) {
            return processFailed(LockWatchStateUpdate.failed(success.logId()));
        }
    }
}
