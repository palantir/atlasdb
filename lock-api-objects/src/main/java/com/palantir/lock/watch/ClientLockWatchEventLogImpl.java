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
import com.google.common.collect.ImmutableSet;
import com.palantir.logsafe.Preconditions;

public final class ClientLockWatchEventLogImpl implements ClientLockWatchEventLog {
    private static final LockWatchStateUpdate.Snapshot FAILED_SNAPSHOT =
            LockWatchStateUpdate.snapshot(UUID.randomUUID(), 0L, ImmutableSet.of(), ImmutableSet.of());

    private final ProcessingVisitor processingVisitor = new ProcessingVisitor();
    private final NewLeaderVisitor newLeaderVisitor = new NewLeaderVisitor();
    private final ConcurrentSkipListMap<Long, LockWatchEvent> eventLog;
    //    private final ConcurrentSkipListSet<Long> processingTime = new ConcurrentSkipListSet<>();
    private volatile IdentifiedVersion identifiedVersion;
    private volatile LockWatchStateUpdate.Snapshot seed = FAILED_SNAPSHOT;

    private ClientLockWatchEventLogImpl() {
        identifiedVersion = IdentifiedVersion.of(UUID.randomUUID(), Optional.empty());
        eventLog = new ConcurrentSkipListMap<>();
    }

    @Override
    public IdentifiedVersion getLatestKnownVersion() {
        return identifiedVersion;
    }

    @Override
    public boolean processUpdate(LockWatchStateUpdate update) {
        if (update.logId().equals(identifiedVersion.id())) {
            update.accept(processingVisitor);
            return false;
        } else {
            update.accept(newLeaderVisitor);
            return true;
        }
    }

    // todo - consider concurrency
    @Override
    public TransactionsLockWatchEvents getEventsForTransactions(
            Map<Long, Long> timestampToVersion,
            IdentifiedVersion version) {
        if (!version.id().equals(identifiedVersion.id())) {
            return TransactionsLockWatchEvents.failure(seed);
        }

        if (eventLog.isEmpty()) {
            return TransactionsLockWatchEvents.success(ImmutableList.of(), timestampToVersion);
        }

        // There is NO guarantee that this is not empty at this current point
        Long oldestVersion = version.version().orElseGet(eventLog::firstKey);
        Long latestVersion = Collections.max(timestampToVersion.values());

        return TransactionsLockWatchEvents.success(
                getEventsBetweenVersions(oldestVersion, latestVersion),
                timestampToVersion);
    }

    // This needs to make sure successes are not being processed at a time before the start version
    private List<LockWatchEvent> getEventsBetweenVersions(long startVersion, long endVersion) {
        Preconditions.checkArgument(startVersion <= endVersion, "startVersion should be before endVersion");
        //        if (processingTime.ceiling(startVersion) != null) {
        // we can't do anything as we are still waiting for processing to happen...
        // could either wait, or throw, or do something I guess
        //        }
        long startKey = eventLog.ceilingKey(startVersion);
        long endKey = eventLog.floorKey(endVersion);
        return new ArrayList<>(eventLog.subMap(startKey, endKey).values());
    }

    // This does not need to be synchronised, as we can have new updates happen while this is still putting in,
    // but we know that events are put by their version (and sorted accordingly),
    // and we know that if a snapshot or failure occurs, we stop immediately
    private void processSuccess(LockWatchStateUpdate.Success success) {
        // Just add events
        IdentifiedVersion localVersion = IdentifiedVersion.of(success.logId(), Optional.of(success.lastKnownVersion()));
        identifiedVersion = localVersion;
        Long minVersion = Collections.min(
                success.events().stream().map(LockWatchEvent::sequence).collect(Collectors.toList()));
        //        processingTime.add(minVersion);

        // using filter is super hacky way of exiting early, e.g. breaking
        success.events().stream().filter(event -> {
            // this ensures that we are only putting events if we have not lost leader
            // i.e. no case where we succeed, then immediately fail, clearing the cache
            // but then are still putting updates
            if (localVersion.id().equals(identifiedVersion.id())) {
                eventLog.put(event.sequence(), event);
                return false;
            } else {
                return true;
            }
        }).findFirst();

        //        processingTime.remove(minVersion);
    }

    // Race condition:
    // thread 1 processes snapshot, sets iV = iV1
    // thread 2 processes snapshot, does everything (i.e iV = iV2, seed = snapshot2)
    // thread 1 sets seed = snapshot1, but iV = iV2. This is a bad state
    // This is why it needs to be synchronised, or at least have some form of concurrency control.
    private synchronized void processSnapshot(LockWatchStateUpdate.Snapshot snapshot) {
        // Nuke, then treat as a created event of everything
        identifiedVersion = IdentifiedVersion.of(snapshot.logId(), Optional.of(snapshot.lastKnownVersion()));
        eventLog.clear();
        //        processingTime.clear();
        seed = snapshot;
    }

    // By extension of the above, this must also be the case.
    private synchronized void processFailed(LockWatchStateUpdate.Failed failed) {
        // Nuke
        identifiedVersion = IdentifiedVersion.of(failed.logId(), Optional.empty());
        eventLog.clear();
        //        processingTime.clear();
        seed = FAILED_SNAPSHOT;
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
            // We process failed as we actually have failed in this case
            // and we discard all new info
            processFailed(LockWatchStateUpdate.failed(success.logId()));
            return null;
        }
    }
}
