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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
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
    private final ClientLockWatchEventLog eventLog;
    private final HashMap<Long, IdentifiedVersion> timestampMap = new HashMap<>();
    private final TreeMultimap<IdentifiedVersion, Long> aliveVersions = TreeMultimap.create();

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
    }

    @Override
    public synchronized CommitUpdate getCommitUpdate(long startTs, long commitTs, LockToken commitLocksToken) {
        IdentifiedVersion startVersion = timestampMap.get(startTs);
        IdentifiedVersion endVersion = timestampMap.get(commitTs);
        Optional<Set<LockDescriptor>> locksTakenOut = eventLog.getEventsBetweenVersions(startVersion, endVersion);
        return locksTakenOut.map(descriptors -> CommitUpdate.invalidateSome(commitTs, descriptors))
                .orElseGet(() -> CommitUpdate.invalidateWatches(commitTs));
    }

    @Override
    public synchronized TransactionsLockWatchEvents getEventsForTransactions(
            Set<Long> startTimestamps,
            Optional<IdentifiedVersion> version) {
        Preconditions.checkArgument(!startTimestamps.isEmpty(), "Cannot get events for empty set of tranasctions");
        return eventLog.getEventsForTransactions(getTimestampToVersionMap(startTimestamps), version);
    }

    @Override
    public synchronized void removeTimestampFromCache(Long timestamp) {
        IdentifiedVersion versionToRemove = timestampMap.get(timestamp);
        if (versionToRemove != null) {
            timestampMap.remove(timestamp);
            aliveVersions.remove(versionToRemove, timestamp);
        }
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
}
