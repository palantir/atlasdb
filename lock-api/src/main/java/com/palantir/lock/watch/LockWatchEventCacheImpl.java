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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

public final class LockWatchEventCacheImpl implements LockWatchEventCache {
    private final ClientLockWatchEventLog lockWatchEventLog;
    private final ConcurrentSkipListMap<Long, IdentifiedVersion> timestampMap = new ConcurrentSkipListMap<>();
    private final Set<Long> markedForDelete = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private volatile Optional<IdentifiedVersion> earliestVersion;
    private volatile Optional<IdentifiedVersion> currentVersion;

    private LockWatchEventCacheImpl(ClientLockWatchEventLog lockWatchEventLog) {
        this.lockWatchEventLog = lockWatchEventLog;
        this.earliestVersion = Optional.empty();
        this.currentVersion = Optional.empty();
    }

    public static LockWatchEventCacheImpl create(ClientLockWatchEventLog eventLog) {
        return new LockWatchEventCacheImpl(eventLog);
    }

    public static LockWatchEventCacheImpl createWithoutCache() {
        return new LockWatchEventCacheImpl(NoOpClientLockWatchEventLog.INSTANCE);
    }

    @Override
    public Optional<IdentifiedVersion> lastKnownVersion() {
        return currentVersion;
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
        if (!markedForDelete.isEmpty()) {
            markedForDelete.forEach(timestampMap::remove);
            markedForDelete.clear();
            earliestVersion = Optional.of(timestampMap.firstEntry().getValue());
        }

        Optional<IdentifiedVersion> latestVersion = lockWatchEventLog.processUpdate(update, earliestVersion);

        if (!(latestVersion.isPresent()
                && currentVersion.isPresent()
                && latestVersion.get().id().equals(currentVersion.get().id()))) {
            timestampMap.clear();
            earliestVersion = Optional.empty();
        }

        currentVersion = latestVersion;

        currentVersion.ifPresent(
                version -> startTimestamps.forEach(timestamp -> timestampMap.put(timestamp, version)));

        if (!earliestVersion.isPresent()) {
            earliestVersion = currentVersion;
        }

        return currentVersion;
    }

    @Override
    public void processUpdate(LockWatchStateUpdate update) {
        currentVersion = lockWatchEventLog.processUpdate(update, earliestVersion);
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
        Map<Long, IdentifiedVersion> timestampToVersion = new HashMap<>();
        // This may be bad in the case that some timestamps are not there;
        // we don't expect that to happen, but perhaps worth documenting anyway.
        startTimestamps.forEach(timestamp -> timestampToVersion.put(timestamp, timestampMap.get(timestamp)));
        return lockWatchEventLog.getEventsForTransactions(timestampToVersion, version);
    }

    @Override
    public void removeTimestampFromCache(Long timestamp) {
        IdentifiedVersion versionToRemove = timestampMap.get(timestamp);
        if (versionToRemove != null) {
            markedForDelete.add(timestamp);
        }
    }
}
