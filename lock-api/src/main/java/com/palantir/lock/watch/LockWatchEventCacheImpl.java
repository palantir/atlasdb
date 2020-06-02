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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.immutables.value.Value;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultimap;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockToken;
import com.palantir.logsafe.Preconditions;

/**
 * Notes on concurrency: all public methods in this class are synchronised: this removes any concern that the timestamp
 * mapping will be modified while also being cleared or read. For processing updates and getting events, this should not
 * have a performance impact as these methods will be called in a single-threaded manner anyway (via an autobatcher),
 * but the method to remove entries is not necessarily called as such, and may cause some impact on performance.
 */
public final class LockWatchEventCacheImpl implements LockWatchEventCache {
    private final ClientLockWatchEventLog eventLog;
    private final HashMap<Long, MapEntry> timestampMap = new HashMap<>();
    private final TreeMultimap<IdentifiedVersion, Long> aliveVersions =
            TreeMultimap.create(IdentifiedVersion.comparator(), Ordering.natural());

    @GuardedBy("this")
    private boolean failed = false;


    private LockWatchEventCacheImpl(ClientLockWatchEventLog eventLog) {
        this.eventLog = eventLog;
    }

    public static LockWatchEventCacheImpl create() {
        return create(ClientLockWatchEventLogImpl.create());
    }

    @VisibleForTesting
    static LockWatchEventCacheImpl create(ClientLockWatchEventLog eventLog) {
        return new LockWatchEventCacheImpl(eventLog);
    }

    @Override
    public Optional<IdentifiedVersion> lastKnownVersion() {
        return eventLog.getLatestKnownVersion();
    }

    @Override
    public synchronized void processStartTransactionsUpdate(
            Set<Long> startTimestamps,
            LockWatchStateUpdate update) {
        ensureNotFailed(() -> {
            Optional<IdentifiedVersion> latestVersion = processEventLogUpdate(update);

            latestVersion.ifPresent(
                    version -> startTimestamps.forEach(timestamp -> {
                        timestampMap.put(timestamp, MapEntry.of(version));
                        aliveVersions.put(version, timestamp);
                    }));
        });
    }

    @Override
    public synchronized void processGetCommitTimestampsUpdate(
            Collection<TransactionUpdate> transactionUpdates,
            LockWatchStateUpdate update) {
        ensureNotFailed(() -> {
            Optional<IdentifiedVersion> latestVersion = processEventLogUpdate(update);

            latestVersion.ifPresent(version ->
                    transactionUpdates.forEach(transactionUpdate -> {
                        MapEntry previousEntry = timestampMap.get(transactionUpdate.startTs());
                        Preconditions.checkNotNull(previousEntry, "Start timestamp missing from cache");
                        timestampMap.replace(transactionUpdate.startTs(), previousEntry.withCommitInfo(
                                CommitInfo.of(transactionUpdate.commitTs(),
                                        transactionUpdate.writesToken(),
                                        version)));
                    }));
        });
    }

    @Override
    public synchronized CommitUpdate getCommitUpdate(long startTs) {
        checkNotFailed();
        MapEntry entry = timestampMap.get(startTs);
        Preconditions.checkState(entry.commitInfo().isPresent(), "Commit timestamp update not yet processed");
        CommitInfo commitInfo = entry.commitInfo().get();

        TransactionsLockWatchEvents update =
                eventLog.getEventsBetweenVersions(Optional.of(entry.version()), commitInfo.commitVersion()).build();

        if (update.clearCache()) {
            return ImmutableInvalidateAll.builder().build();
        }

        return getCommitUpdate(commitInfo, update.events());
    }

    @Override
    public synchronized TransactionsLockWatchEvents getEventsForTransactions(
            Set<Long> startTimestamps,
            Optional<IdentifiedVersion> startVersion) {
        checkNotFailed();
        Preconditions.checkArgument(!startTimestamps.isEmpty(), "Cannot get events for empty set of tranasctions");
        Map<Long, IdentifiedVersion> timestampToVersion = getTimestampToVersionMap(startTimestamps);
        IdentifiedVersion endVersion = Collections.max(timestampToVersion.values(), IdentifiedVersion.comparator());
        return eventLog.getEventsBetweenVersions(startVersion, endVersion)
                .startTsToSequence(timestampToVersion)
                .build();
    }

    @Override
    public synchronized void removeTransactionStateFromCache(long startTimestamp) {
        ensureNotFailed(() -> {
            MapEntry entryToRemove = timestampMap.get(startTimestamp);
            if (entryToRemove != null) {
                timestampMap.remove(startTimestamp);
                aliveVersions.remove(entryToRemove.version(), startTimestamp);
            }
        });
    }

    private synchronized CommitUpdate getCommitUpdate(CommitInfo commitInfo, List<LockWatchEvent> events) {
        LockEventVisitor eventVisitor = new LockEventVisitor(commitInfo.commitLockToken());
        Set<LockDescriptor> locksTakenOut = new HashSet<>();
        events.forEach(event -> locksTakenOut.addAll(event.accept(eventVisitor)));
        return ImmutableInvalidateSome.builder().invalidatedLocks(locksTakenOut).build();
    }

    private synchronized Optional<IdentifiedVersion> processEventLogUpdate(LockWatchStateUpdate update) {
        Optional<IdentifiedVersion> currentVersion = eventLog.getLatestKnownVersion();
        Optional<IdentifiedVersion> latestVersion = eventLog.processUpdate(update);
        getEarliestVersion().ifPresent(eventLog::removeOldEntries);

        if (!(latestVersion.isPresent()
                && currentVersion.isPresent()
                && latestVersion.get().id().equals(currentVersion.get().id())
                && update.accept(SuccessVisitor.INSTANCE))) {
            timestampMap.clear();
            aliveVersions.clear();
        }
        return latestVersion;
    }

    @VisibleForTesting
    synchronized Map<Long, IdentifiedVersion> getTimestampToVersionMap(Set<Long> startTimestamps) {
        Map<Long, IdentifiedVersion> timestampToVersion = new HashMap<>();
        startTimestamps.forEach(timestamp -> {
            MapEntry entry = timestampMap.get(timestamp);
            Preconditions.checkNotNull(entry, "Timestamp missing from cache");
            timestampToVersion.put(timestamp, entry.version());
        });
        return timestampToVersion;
    }

    @VisibleForTesting
    synchronized Optional<IdentifiedVersion> getEarliestVersion() {
        if (aliveVersions.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(aliveVersions.keySet().first());
        }
    }

    private synchronized void ensureNotFailed(Runnable runnable) {
        checkNotFailed();
        failed = true;
        runnable.run();
        failed = false;
    }

    private synchronized void checkNotFailed() {
        Preconditions.checkState(!failed, "Log is in an inconsistent state");
    }

    enum SuccessVisitor implements LockWatchStateUpdate.Visitor<Boolean> {
        INSTANCE;

        @Override
        public Boolean visit(LockWatchStateUpdate.Failed failed) {
            return false;
        }

        @Override
        public Boolean visit(LockWatchStateUpdate.Success success) {
            return true;
        }

        @Override
        public Boolean visit(LockWatchStateUpdate.Snapshot snapshot) {
            return false;
        }
    }

    private static final class LockEventVisitor implements LockWatchEvent.Visitor<Set<LockDescriptor>> {
        private final LockToken commitLocksToken;

        private LockEventVisitor(LockToken commitLocksToken) {
            this.commitLocksToken = commitLocksToken;
        }

        @Override
        public Set<LockDescriptor> visit(LockEvent lockEvent) {
            if (lockEvent.lockToken().equals(commitLocksToken)) {
                return ImmutableSet.of();
            } else {
                return lockEvent.lockDescriptors();
            }
        }

        @Override
        public Set<LockDescriptor> visit(UnlockEvent unlockEvent) {
            return ImmutableSet.of();
        }

        @Override
        public Set<LockDescriptor> visit(LockWatchCreatedEvent lockWatchCreatedEvent) {
            return lockWatchCreatedEvent.lockDescriptors();
        }
    }

    @Value.Immutable
    interface MapEntry {
        @Value.Parameter
        IdentifiedVersion version();

        @Value.Parameter
        Optional<CommitInfo> commitInfo();

        static MapEntry of(IdentifiedVersion version) {
            return ImmutableMapEntry.of(version, Optional.empty());
        }

        default MapEntry withCommitInfo(CommitInfo commitInfo) {
            return ImmutableMapEntry.builder().from(this).commitInfo(commitInfo).build();
        }
    }

    @Value.Immutable
    interface CommitInfo {
        @Value.Parameter
        long commitTs();

        @Value.Parameter
        LockToken commitLockToken();

        @Value.Parameter
        IdentifiedVersion commitVersion();

        static CommitInfo of(long commitTs, LockToken commitLockToken, IdentifiedVersion commitVersion) {
            return ImmutableCommitInfo.of(commitTs, commitLockToken, commitVersion);
        }
    }
}
