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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;

public final class ClientLockWatchEventLogImpl implements ClientLockWatchEventLog {
    private static final Comparator<IdentifiedVersion> comparator = (identifiedVersion1, identifiedVersion2) -> {
        Optional<Long> version1 = identifiedVersion1.version();
        Optional<Long> version2 = identifiedVersion2.version();

        if (version1.equals(version2)) {
            return 0;
        }

        if (!version1.isPresent()) {
            return -1;
        }

        if (!version2.isPresent()) {
            return 1;
        }

        return version1.get() < version2.get() ? -1 : 1;
    };
    private final ProcessingVisitor processingVisitor = new ProcessingVisitor();
    private final NewLeaderVisitor newLeaderVisitor = new NewLeaderVisitor();
    private volatile LogData logData = new LogData();

    public ClientLockWatchEventLogImpl() {
        //        identifiedVersion = IdentifiedVersion.of(UUID.randomUUID(), Optional.empty());
        //        eventLog = new ConcurrentSkipListMap<>();
    }

    @Override
    public Optional<TransactionsLockWatchEvents> getEventsForTransactions(
            Map<Long, IdentifiedVersion> timestampToVersion,
            IdentifiedVersion version) {
        LogData currentData = logData;
        // case 1: their version has no version: tell them to go and get a snapshot.
        if (!version.version().isPresent()) {
            return Optional.empty();
        }

        // case 2: their version is too far behind our log: tell them to go and get a snapshot.
        if (!currentData.eventLog.containsKey(version)) {
            return Optional.empty();
        }

        // case 3: their version has a different uuid to yours (leader election): tell them to get a snapshot.
        if (!version.id().equals(currentData.latestVersion.id())) {
            return Optional.empty();
        }

        // So now we know that they are reasonably up-to-date, and on the same leader as us.
        // Therefore lets go and compute something useful.
        IdentifiedVersion latestVersion = Collections.max(timestampToVersion.values(), comparator);
        List<LockWatchEvent> events = new ArrayList<>(currentData.eventLog.subMap(version, latestVersion).values());
        return Optional.of(TransactionsLockWatchEvents.success(events, timestampToVersion));
    }

    @Override
    public IdentifiedVersion getLatestKnownVersion() {
        return logData.latestVersion;
    }

    @Override
    public IdentifiedVersion processUpdate(LockWatchStateUpdate update) {
        if (update.logId().equals(logData.latestVersion.id())) {
            update.accept(processingVisitor);
        } else {
            update.accept(newLeaderVisitor);
        }
        return logData.latestVersion;
    }

    private synchronized void processSuccess(LockWatchStateUpdate.Success success) {
        IdentifiedVersion latestVersion = IdentifiedVersion.of(success.logId(),
                Optional.of(success.lastKnownVersion()));
        updateLatestVersion(latestVersion);
        success.events().forEach(event -> logData.eventLog.put(latestVersion, event));
    }

    private synchronized void processSnapshot(LockWatchStateUpdate.Snapshot snapshot) {
        resetLog(IdentifiedVersion.of(snapshot.logId(),
                Optional.of(snapshot.lastKnownVersion())));
    }

    private synchronized void processFailed(LockWatchStateUpdate.Failed failed) {
        resetLog(IdentifiedVersion.of(failed.logId(), Optional.empty()));
    }

    private void resetLog(IdentifiedVersion latestVersion) {
        logData = new LogData(latestVersion);
    }

    private void updateLatestVersion(IdentifiedVersion latestVersion) {
        // todo - implement.
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

    private static final class LogData {
        private final ConcurrentSkipListMap<IdentifiedVersion, LockWatchEvent> eventLog;
        private volatile IdentifiedVersion latestVersion;

        private LogData(IdentifiedVersion version) {
            eventLog = new ConcurrentSkipListMap<>(comparator);
            latestVersion = version;
        }

        private LogData() {
            this(IdentifiedVersion.of(UUID.randomUUID(), Optional.empty()));
        }
    }
}
