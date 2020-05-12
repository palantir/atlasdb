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
    private final ProcessingVisitor processingVisitor = new ProcessingVisitor();
    private final NewLeaderVisitor newLeaderVisitor = new NewLeaderVisitor();
    private volatile IdentifiedVersion identifiedVersion; // todo - is this sufficient for concurrency?
    private ConcurrentSkipListMap<Long, LockWatchEvent> eventLog;
    private volatile LockWatchStateUpdate.Snapshot seed = null;

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
            update.accept(newLeaderVisitor);
        } else {
            update.accept(processingVisitor);
        }
    }

    // todo - consider concurrency
    @Override
    public TransactionsLockWatchEvents getEventsForTransactions(
            Map<Long, Long> timestampToVersion,
            IdentifiedVersion version) {
        if (!version.id().equals(identifiedVersion.id())) {
            // todo - we need to indicate that we cannot do this,
            //      and return a forced snapshot
            //      need to determine how we can actually get a forced snapshot out from this, if it makes sense
            return TransactionsLockWatchEvents.failure(seed);
        }

        Long oldestVersion = version.version().orElseGet(() -> eventLog.firstKey());
        Long latestVersion = Collections.max(timestampToVersion.values());

        return TransactionsLockWatchEvents.success(
                getEventsBetweenVersions(oldestVersion, latestVersion),
                timestampToVersion);
    }

    private List<LockWatchEvent> getEventsBetweenVersions(long startVersion, long endVersion) {
        long startKey = eventLog.ceilingKey(startVersion);
        long endKey = eventLog.floorKey(endVersion);
        return new ArrayList<>(eventLog.subMap(startKey, endKey).values());
    }

    private void processSuccess(LockWatchStateUpdate.Success success) {
        // Just add events
        identifiedVersion = IdentifiedVersion.of(success.logId(), Optional.of(success.lastKnownVersion()));
        IdentifiedVersion localVersion = identifiedVersion;
        success.events().forEach(event -> {
            // this ensures that we are only putting events if we have not lost leader
            // i.e. no case where we succeed, then immediately fail, clearing the cache
            // but then are still putting updates
            if (localVersion.id().equals(identifiedVersion.id())) {
                eventLog.put(event.sequence(), event);
            }
        });
    }

    private void processSnapshot(LockWatchStateUpdate.Snapshot snapshot) {
        // Nuke, then treat as a created event of everything
        identifiedVersion = IdentifiedVersion.of(snapshot.logId(), Optional.of(snapshot.lastKnownVersion()));
        eventLog.clear();
        seed = snapshot;
//        LockWatchEvent recreatedEvent = LockWatchCreatedEvent.builder(snapshot.lockWatches(), snapshot.locked())
//                .build(snapshot.lastKnownVersion());
//        eventLog.put(recreatedEvent.sequence(), recreatedEvent);
    }

    private void processFailed(LockWatchStateUpdate.Failed failed) {
        // Nuke
        // todo - is this thread safe? what happens mid-way through a clear? or if multiple try to clear?
        identifiedVersion = IdentifiedVersion.of(failed.logId(), Optional.empty());
        eventLog.clear();
        seed = null;
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

    private class NewLeaderVisitor implements LockWatchStateUpdate.Visitor<Void> {

        @Override
        public Void visit(LockWatchStateUpdate.Failed failed) {
            processFailed(failed);
            return null;
        }

        @Override
        public Void visit(LockWatchStateUpdate.Success success) {
            // throw may be heavy handed, but do it for now - that or treat as if it failed
            throw new RuntimeException();
        }

        @Override
        public Void visit(LockWatchStateUpdate.Snapshot snapshot) {
            processSnapshot(snapshot);
            return null;
        }
    }
}
