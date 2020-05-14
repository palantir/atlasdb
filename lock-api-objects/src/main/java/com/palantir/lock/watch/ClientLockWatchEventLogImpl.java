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
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.logsafe.Preconditions;

public final class ClientLockWatchEventLogImpl implements ClientLockWatchEventLog {
    private final ProcessingVisitor processingVisitor = new ProcessingVisitor();
    private final NewLeaderVisitor newLeaderVisitor = new NewLeaderVisitor();
    private final ConcurrentSkipListMap<Long, LockWatchEvent> eventLog;
    private volatile IdentifiedVersion identifiedVersion;
    private LockWatchStateUpdate.Snapshot seed = failedSnapshot(UUID.randomUUID());

    public ClientLockWatchEventLogImpl() {
        identifiedVersion = IdentifiedVersion.of(UUID.randomUUID(), Optional.empty());
        eventLog = new ConcurrentSkipListMap<>();
    }

    @Override
    public IdentifiedVersion getLatestKnownVersion() {
        return identifiedVersion;
    }

    @Override
    public IdentifiedVersion processUpdate(LockWatchStateUpdate update) {
        if (update.logId().equals(identifiedVersion.id())) {
            update.accept(processingVisitor);
            return identifiedVersion;
        } else {
            update.accept(newLeaderVisitor);
            return identifiedVersion;
        }
    }

    @Override
    public TransactionsLockWatchEvents getEventsForTransactions(
            Map<Long, Long> timestampToVersion,
            IdentifiedVersion version) {
        if (!version.id().equals(identifiedVersion.id())) {
            return TransactionsLockWatchEvents.failure(seed);
        }

        Long latestVersion = Collections.max(timestampToVersion.values());
        ConcurrentSkipListMap<Long, LockWatchEvent> logSnapshot = eventLog.clone();

        if (logSnapshot.isEmpty()) {
            return TransactionsLockWatchEvents.success(ImmutableList.of(), timestampToVersion);
        }

        // if version is old / empty, you need snapshot

        long oldestVersion = version.version().orElseGet(logSnapshot::firstKey);
        Preconditions.checkArgument(oldestVersion <= latestVersion, "startVersion should be before endVersion");

        long startKey = logSnapshot.ceilingKey(oldestVersion);
        long endKey = logSnapshot.floorKey(latestVersion);

        return TransactionsLockWatchEvents.success(
                new ArrayList<>(logSnapshot.subMap(startKey, endKey).values()),
                timestampToVersion);
    }

    private synchronized void processSuccess(LockWatchStateUpdate.Success success) {
        setToLatestVersion(IdentifiedVersion.of(success.logId(), Optional.of(success.lastKnownVersion())));
        success.events().forEach(event -> eventLog.put(event.sequence(), event));
    }

    private synchronized void processSnapshot(LockWatchStateUpdate.Snapshot snapshot) {
        setToLatestVersion(IdentifiedVersion.of(snapshot.logId(), Optional.of(snapshot.lastKnownVersion())));
        eventLog.clear();
        seed = snapshot;
    }

    private synchronized void processFailed(LockWatchStateUpdate.Failed failed) {
        setToLatestVersion(IdentifiedVersion.of(failed.logId(), Optional.empty()));
        eventLog.clear();
        seed = failedSnapshot(failed.logId());
    }

    private static LockWatchStateUpdate.Snapshot failedSnapshot(UUID uuid) {
        return LockWatchStateUpdate.snapshot(uuid, -1L, ImmutableSet.of(), ImmutableSet.of());
    }

    private void setToLatestVersion(IdentifiedVersion newVersion) {
        if (!identifiedVersion.version().isPresent()) {
            identifiedVersion = newVersion;
            return;
        }

        long current = identifiedVersion.version().get();
        if (!newVersion.version().isPresent()) {
            identifiedVersion = newVersion;
            return;
        }

        long version = newVersion.version().get();
        if (version >= current) {
            identifiedVersion = newVersion;
        }
    }

    private class ProcessingVisitor implements LockWatchStateUpdate.Visitor<Void> {
        @Override
        public Void visit(LockWatchStateUpdate.Failed failed) {
            processFailed(failed);
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

    private class NewLeaderVisitor extends ProcessingVisitor {
        @Override
        public Void visit(LockWatchStateUpdate.Success success) {
            processFailed(LockWatchStateUpdate.failed(success.logId()));
            return null;
        }
    }
}
