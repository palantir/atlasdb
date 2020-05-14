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
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.logsafe.Preconditions;

public final class ClientLockWatchEventLogImpl implements ClientLockWatchEventLog {
    private final ProcessingVisitor processingVisitor = new ProcessingVisitor();
    private final NewLeaderVisitor newLeaderVisitor = new NewLeaderVisitor();
    private final ConcurrentSkipListMap<Long, LockWatchEvent> eventLog;
    //    private final ConcurrentSkipListSet<Long> processingUpTo = new ConcurrentSkipListSet<>();
    private volatile IdentifiedVersion identifiedVersion;
    private volatile LockWatchStateUpdate.Snapshot seed = failedSnapshot(UUID.randomUUID());

    private ClientLockWatchEventLogImpl() {
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
        if (eventLog.isEmpty()) {
            return TransactionsLockWatchEvents.success(ImmutableList.of(), timestampToVersion);
        }

        // The clearing of the event log can race - we could just take a snapshot of it,
        // but unsure how performant that is.

        // could have been cleared in between, so we need to verify the outcome is not null
        Long oldestVersion = version.version().orElseGet(eventLog::firstKey);

        // this can only happen if the log was cleared since we checked it
        if (oldestVersion == null) {
            return TransactionsLockWatchEvents.failure(seed);
        }

        Preconditions.checkArgument(oldestVersion <= latestVersion, "startVersion should be before endVersion");
        Long startKey = eventLog.ceilingKey(oldestVersion);
        Long endKey = eventLog.floorKey(latestVersion);
        // race condition - event log cleared while calling this.
        if (startKey == null || endKey == null) {
            return TransactionsLockWatchEvents.failure(seed);
        } else {
            return TransactionsLockWatchEvents.success(new ArrayList<>(eventLog.subMap(startKey, endKey).values()),
                    timestampToVersion);
        }
    }

    //    private boolean eventsBeforeTimestampAreImmutable(Long latestVersion) {
    //        if (identifiedVersion.version().isPresent() && latestVersion <= identifiedVersion.version().get()) {
    //            return processingUpTo.floor(latestVersion) == null;
    //        }
    //
    //        return false;
    //    }

    // todo - consider a way to make this not synchronised
    private synchronized void processSuccess(LockWatchStateUpdate.Success success) {
        // Just add events
        IdentifiedVersion localVersion = IdentifiedVersion.of(success.logId(), Optional.of(success.lastKnownVersion()));
        identifiedVersion = localVersion;
        Long minVersion = Collections.min(
                success.events().stream().map(LockWatchEvent::sequence).collect(Collectors.toList()));
        //        processingUpTo.add(minVersion);
        success.events().forEach(event -> eventLog.put(event.sequence(), event));
        //        processingUpTo.remove(minVersion);
    }

    // Race condition:
    // thread 1 processes snapshot, sets iV = iV1
    // thread 2 processes snapshot, does everything (i.e iV = iV2, seed = snapshot2)
    // thread 1 sets seed = snapshot1, but iV = iV2. This is a bad state
    // This is why it needs to be synchronised, or at least have some form of concurrency control.
    private synchronized void processSnapshot(LockWatchStateUpdate.Snapshot snapshot) {
        identifiedVersion = IdentifiedVersion.of(snapshot.logId(), Optional.of(snapshot.lastKnownVersion()));
        eventLog.clear();
        seed = snapshot;
    }

    // By extension of the above, this must also be the case.
    private synchronized void processFailed(LockWatchStateUpdate.Failed failed) {
        // an empty version means that the next request to TL will give us a snapshot to re-seed.
        identifiedVersion = IdentifiedVersion.of(failed.logId(), Optional.empty());
        eventLog.clear();
        seed = failedSnapshot(failed.logId());
    }

    // a failed snapshot - i.e. we failed to get an update, or even a snapshot, so essentially
    // resets the state of everything without needing a special third response.
    private static LockWatchStateUpdate.Snapshot failedSnapshot(UUID uuid) {
        return LockWatchStateUpdate.snapshot(uuid, -1L, ImmutableSet.of(), ImmutableSet.of());
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
            // We process success as a fail as we actually have failed in this case as the leader has changed but
            // we have not got a snapshot, and we discard all info.
            processFailed(LockWatchStateUpdate.failed(success.logId()));
            return null;
        }
    }
}
