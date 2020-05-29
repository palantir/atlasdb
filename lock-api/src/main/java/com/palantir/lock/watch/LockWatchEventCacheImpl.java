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

import javax.annotation.concurrent.GuardedBy;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultimap;
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
    @GuardedBy("this")
    private final ClientLockWatchEventLog eventLog;
    @GuardedBy("this")
    private final HashMap<Long, IdentifiedVersion> timestampMap = new HashMap<>();
    @GuardedBy("this")
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
    public synchronized void processTransactionUpdate(
            Collection<Long> startTimestamps,
            LockWatchStateUpdate update) {
        checkNotFailed();
        failed = true;
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

        latestVersion.ifPresent(
                version -> startTimestamps.forEach(timestamp -> {
                    timestampMap.put(timestamp, version);
                    aliveVersions.put(version, timestamp);
                }));
        failed = false;
    }

    @Override
    public synchronized CommitUpdate getCommitUpdate(long startTs, long commitTs, LockToken commitLocksToken) {
        checkNotFailed();
        IdentifiedVersion startVersion = timestampMap.get(startTs);
        IdentifiedVersion endVersion = timestampMap.get(commitTs);
        ClientEventUpdate update = eventLog.getEventsForTransactions(Optional.of(startVersion), endVersion);
        return update.accept(new ClientEventVisitor(commitTs));
    }

    @Override
    public synchronized TransactionsLockWatchEvents getEventsForTransactions(
            Set<Long> startTimestamps,
            Optional<IdentifiedVersion> startVersion) {
        checkNotFailed();
        Preconditions.checkArgument(!startTimestamps.isEmpty(), "Cannot get events for empty set of tranasctions");
        Map<Long, IdentifiedVersion> timestampToVersion = getTimestampToVersionMap(startTimestamps);
        IdentifiedVersion endVersion = Collections.max(timestampToVersion.values(), IdentifiedVersion.comparator());
        return eventLog.getEventsForTransactions(startVersion, endVersion).map(timestampToVersion);
    }

    @Override
    public synchronized void removeTimestampFromCache(long timestamp) {
        checkNotFailed();
        failed = true;
        IdentifiedVersion versionToRemove = timestampMap.get(timestamp);
        if (versionToRemove != null) {
            timestampMap.remove(timestamp);
            aliveVersions.remove(versionToRemove, timestamp);
        }
        failed = false;
    }

    @VisibleForTesting
    Map<Long, IdentifiedVersion> getTimestampToVersionMap(Set<Long> startTimestamps) {
        Map<Long, IdentifiedVersion> timestampToVersion = new HashMap<>();
        startTimestamps.forEach(timestamp -> {
            IdentifiedVersion version = timestampMap.get(timestamp);
            Preconditions.checkNotNull(version, "Timestamp missing from cache");
            timestampToVersion.put(timestamp, version);
        });
        return timestampToVersion;
    }

    @VisibleForTesting
    Optional<IdentifiedVersion> getEarliestVersion() {
        if (aliveVersions.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(aliveVersions.keySet().first());
        }
    }

    private void checkNotFailed() {
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

    private static final class ClientEventVisitor implements ClientEventUpdate.Visitor<CommitUpdate> {
        private final long commitTs;

        private ClientEventVisitor(long commitTs) {
            this.commitTs = commitTs;
        }

        @Override
        public CommitUpdate visit(ClientEventUpdate.ClientEvents events) {
            List<LockWatchEvent> eventList = events.events();
            Set<LockDescriptor> locksTakenOut = new HashSet<>();
            eventList.forEach(event -> locksTakenOut.addAll(event.accept(LockEventVisitor.INSTANCE)));
            return CommitUpdate.invalidateSome(commitTs, locksTakenOut);
        }

        @Override
        public CommitUpdate visit(ClientEventUpdate.ClientSnapshot snapshot) {
            return CommitUpdate.invalidateWatches(commitTs);
        }
    }

    enum LockEventVisitor implements LockWatchEvent.Visitor<Set<LockDescriptor>> {
        INSTANCE;

        @Override
        public Set<LockDescriptor> visit(LockEvent lockEvent) {
            return lockEvent.lockDescriptors();
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
}
