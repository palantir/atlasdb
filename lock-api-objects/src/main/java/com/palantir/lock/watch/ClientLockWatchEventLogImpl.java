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

public final class ClientLockWatchEventLogImpl implements ClientLockWatchEventLog {
    private final ProcessingVisitor visitor = new ProcessingVisitor();
    private volatile IdentifiedVersion identifiedVersion; // todo - is this sufficient?
    private ConcurrentSkipListMap<Long, LockWatchEvent> eventLog;

    private ClientLockWatchEventLogImpl() {
        // todo - determine how to init version
        identifiedVersion = IdentifiedVersion.of(UUID.randomUUID(), Optional.empty());
        eventLog = new ConcurrentSkipListMap<>();
    }

    @Override
    public IdentifiedVersion getLatestKnownVersion() {
        return identifiedVersion;
    }

    // todo - Consideration - concurrency
    @Override
    public void processUpdate(LockWatchStateUpdate update) {
        if (!update.logId().equals(identifiedVersion.id())) {
            // This is the case where we have changed leader
            // In case of fail, reset version;
            identifiedVersion = IdentifiedVersion.of(update.logId(), Optional.empty());
            // todo - is this thread safe? what happens mid-way through a clear? or if multiple try to clear?
            eventLog.clear();
            // In case of snapshot, we need to reset the cache
            // in case of success, probably shouldn't have that here.
        }

        // processes in case of snapshot or success
        update.accept(visitor);
    }

    private void processSuccess(LockWatchStateUpdate.Success success) {
        identifiedVersion = IdentifiedVersion.of(success.logId(), Optional.of(success.lastKnownVersion()));
        IdentifiedVersion localVersion = identifiedVersion;
        success.events().forEach(event -> {
            // this ensures that we are only putting events if we have not lost leader
            // i.e. no case where we succeed, then immediately fail, clearing the cache
            // but then are still putting updates
            if(localVersion.id().equals(identifiedVersion.id())) {
                eventLog.put(event.sequence(), event);
            }
        });
    }

    private void processSnapshot(LockWatchStateUpdate.Snapshot snapshot) {

    }

    private void processFailed(LockWatchStateUpdate.Failed failed) {
    }

    // todo - consider concurrency
    @Override
    public TransactionsLockWatchEvents getEventsForTransactions(
            Map<Long, Long> timestampToVersion,
            IdentifiedVersion version) {
        if(!version.id().equals(getLatestKnownVersion().id())) {
            // todo - we need to indicate that we cannot do this,
            //      and return a forced snapshot
            return null;
        }

        Long oldestVersion = version.version().orElseGet(() -> eventLog.firstKey());
        Long latestVersion = Collections.max(timestampToVersion.values());

        Long firstKey = eventLog.ceilingKey(oldestVersion);
        Long lastKey = eventLog.floorKey(latestVersion);

        List<LockWatchEvent> events = new ArrayList<>(eventLog.subMap(firstKey, lastKey).values());
        return TransactionsLockWatchEvents.success(events, timestampToVersion);
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

    private static class AssertSuccessVisitor implements LockWatchStateUpdate.Visitor<LockWatchStateUpdate.Success> {
        @Override
        public LockWatchStateUpdate.Success visit(LockWatchStateUpdate.Failed failed) {
            throw fail("Failed update");
        }

        @Override
        public LockWatchStateUpdate.Success visit(LockWatchStateUpdate.Success success) {
            return success;
        }

        @Override
        public LockWatchStateUpdate.Success visit(LockWatchStateUpdate.Snapshot snapshot) {
            throw fail("Unexpected snapshot");
        }

    }

    public static class AssertSnapshotVisitor implements LockWatchStateUpdate.Visitor<LockWatchStateUpdate.Snapshot> {
        @Override
        public LockWatchStateUpdate.Snapshot visit(LockWatchStateUpdate.Failed failed) {
            throw fail("Failed update");
        }

        @Override
        public LockWatchStateUpdate.Snapshot visit(LockWatchStateUpdate.Success success) {
            throw fail("Unexpected success");
        }

        @Override
        public LockWatchStateUpdate.Snapshot visit(LockWatchStateUpdate.Snapshot snapshot) {
            return snapshot;
        }
    }

    private static RuntimeException fail(String message) {
        return new RuntimeException(message);
    }
}
