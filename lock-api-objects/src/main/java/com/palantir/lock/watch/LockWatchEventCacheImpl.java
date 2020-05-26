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

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

public final class LockWatchEventCacheImpl implements LockWatchEventCache {
    private final ClientLockWatchEventLog lockWatchEventLog;
    private final Cache<Long, IdentifiedVersion> timestampCache = Caffeine.newBuilder()
            .build();
    private final ConcurrentSkipListMap<IdentifiedVersion, Long> markedForDelete = new ConcurrentSkipListMap<>();
    private volatile Optional<IdentifiedVersion> earliestVersion;
    private volatile Optional<IdentifiedVersion> currentVersion;

    private LockWatchEventCacheImpl(ClientLockWatchEventLog lockWatchEventLog) {
        this.lockWatchEventLog = lockWatchEventLog;
        this.earliestVersion = Optional.empty();
    }

    public static LockWatchEventCacheImpl create() {
        return new LockWatchEventCacheImpl(NoOpClientLockWatchEventLog.INSTANCE);
    }

    @Override
    public Optional<IdentifiedVersion> lastKnownVersion() {
        return currentVersion;
    }

    /**
     * Notes on Concurrency: This should only be called in a single-threaded manner, on the transaction starting flow.
     * Therefore, we force it to be synchronized so that changes to the cache do not cause a race condition. Deletes
     * from the cache and underlying log are handled in this method; there is no concern that they will grow large
     * between calls of this method as they are never added to elsewhere.
     *
     * @return
     */
    @Override
    public synchronized Optional<IdentifiedVersion> processStartTransactionsUpdate(
            Set<Long> startTimestamps,
            LockWatchStateUpdate update) {
        earliestVersion = Optional.of(markedForDelete.lastKey());
        markedForDelete.forEach((version, $) -> timestampCache.invalidate(version));
        markedForDelete.clear();

        Optional<IdentifiedVersion> latestVersion = lockWatchEventLog.processUpdate(update, earliestVersion);

        if (!(latestVersion.isPresent()
                && currentVersion.isPresent()
                && latestVersion.get().id().equals(currentVersion.get().id()))) {
            timestampCache.invalidateAll();
        }
        currentVersion = latestVersion;

        currentVersion.ifPresent(
                version -> startTimestamps.forEach(timestamp -> timestampCache.put(timestamp, version)));
        return currentVersion;
    }

    @Override
    public void processUpdate(LockWatchStateUpdate update) {
        lockWatchEventLog.processUpdate(update, earliestVersion);
    }

    /**
     * This is also synchronised for the call to getAllPresent. This will also be called in a single-threaded way - once
     * per batch on the start transaction codepath.
     */
    @Override
    public synchronized TransactionsLockWatchEvents getEventsForTransactions(
            Set<Long> startTimestamps,
            Optional<IdentifiedVersion> version) {
        Map<Long, IdentifiedVersion> timestampToVersion = timestampCache.getAllPresent(startTimestamps);
        return lockWatchEventLog.getEventsForTransactions(timestampToVersion, version);
    }

    @Override
    public void removeTimestampFromCache(Long timestamp) {
        IdentifiedVersion versionToRemove = timestampCache.getIfPresent(timestamp);
        timestampCache.invalidate(timestamp);
        if (versionToRemove != null) {
            markedForDelete.put(versionToRemove, timestamp);
        }
    }
}
