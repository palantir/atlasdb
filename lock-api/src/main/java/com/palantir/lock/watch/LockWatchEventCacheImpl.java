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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.TreeMultimap;
import com.palantir.logsafe.Preconditions;

public final class LockWatchEventCacheImpl implements LockWatchEventCache {
    private final ClientLockWatchEventLog lockWatchEventLog;
    private final ConcurrentSkipListMap<Long, IdentifiedVersion> timestampMap = new ConcurrentSkipListMap<>();
    private final TreeMultimap<IdentifiedVersion, Long> aliveVersions = TreeMultimap.create();

    private LockWatchEventCacheImpl(ClientLockWatchEventLog lockWatchEventLog) {
        this.lockWatchEventLog = lockWatchEventLog;
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
        return lockWatchEventLog.getLatestKnownVersion();
    }

    /**
     * Notes on concurrency: This should only be called in a single-threaded manner, on the transaction starting flow.
     * Therefore, forcing it to be synchronised does not incur a performance hit but guarantees that changes to the
     * cache do not cause a race condition. Deletes from the cache and underlying log are handled in this method; there
     * is no concern that they will grow large between calls of this method as they are never added to elsewhere.
     */
    @Override
    public synchronized Optional<IdentifiedVersion> processStartTransactionsUpdate(
            Set<Long> startTimestamps,
            LockWatchStateUpdate update) {
        Optional<IdentifiedVersion> currentVersion = lockWatchEventLog.getLatestKnownVersion();
        Optional<IdentifiedVersion> latestVersion = lockWatchEventLog.processUpdate(update, getEarliestVersion());

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
        return latestVersion;
    }

    @Override
    public synchronized void processUpdate(LockWatchStateUpdate update) {
        lockWatchEventLog.processUpdate(update, getEarliestVersion());
    }

    /**
     * This is synchronised for the call to getAllPresent. This will also be called in a single-threaded way - once per
     * batch on the start transaction codepath, and since that is the same codepath as the other processing method,
     * should not cause bad performance.
     */
    @Override
    public synchronized TransactionsLockWatchEvents getEventsForTransactions(
            Set<Long> startTimestamps,
            Optional<IdentifiedVersion> version) {
        return lockWatchEventLog.getEventsForTransactions(getTimestampToVersionMap(startTimestamps), version);
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
            return Optional.ofNullable(aliveVersions.keySet().first());
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
