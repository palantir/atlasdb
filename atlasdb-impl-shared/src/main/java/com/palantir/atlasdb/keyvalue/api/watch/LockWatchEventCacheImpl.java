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

package com.palantir.atlasdb.keyvalue.api.watch;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.keyvalue.api.ResilientLockWatchProxy;
import com.palantir.atlasdb.keyvalue.api.cache.CacheMetrics;
import com.palantir.atlasdb.keyvalue.api.watch.TimestampStateStore.CommitInfo;
import com.palantir.atlasdb.transaction.api.TransactionLockWatchFailedException;
import com.palantir.lock.watch.CommitUpdate;
import com.palantir.lock.watch.LockWatchEventCache;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.lock.watch.NoOpLockWatchEventCache;
import com.palantir.lock.watch.TransactionUpdate;
import com.palantir.lock.watch.TransactionsLockWatchUpdate;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public final class LockWatchEventCacheImpl implements LockWatchEventCache {
    // The minimum number of events should be the same as Timelocks' LockEventLogImpl.
    private static final int MIN_EVENTS = 1000;
    private static final int MAX_EVENTS = 10_000;

    private final LockWatchEventLog eventLog;
    private final TimestampStateStore timestampStateStore;

    public static LockWatchEventCache create(CacheMetrics metrics) {
        return ResilientLockWatchProxy.newEventCacheProxy(
                new LockWatchEventCacheImpl(LockWatchEventLog.create(MIN_EVENTS, MAX_EVENTS)),
                NoOpLockWatchEventCache.create(),
                metrics);
    }

    @VisibleForTesting
    LockWatchEventCacheImpl(LockWatchEventLog eventLog) {
        this.eventLog = eventLog;
        this.timestampStateStore = new TimestampStateStore();
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public synchronized Optional<LockWatchVersion> lastKnownVersion() {
        return eventLog.getLatestKnownVersion();
    }

    @Override
    public synchronized void processStartTransactionsUpdate(Set<Long> startTimestamps, LockWatchStateUpdate update) {
        Optional<LockWatchVersion> updateVersion = processEventLogUpdate(update);
        updateVersion.ifPresent(version -> timestampStateStore.putStartTimestamps(startTimestamps, version));
    }

    @Override
    public synchronized void processGetCommitTimestampsUpdate(
            Collection<TransactionUpdate> transactionUpdates, LockWatchStateUpdate update) {
        Optional<LockWatchVersion> updateVersion = processEventLogUpdate(update);
        updateVersion.ifPresent(version -> timestampStateStore.putCommitUpdates(transactionUpdates, version));
    }

    @Override
    public synchronized CommitUpdate getCommitUpdate(long startTs) {
        Optional<LockWatchVersion> startVersion = timestampStateStore.getStartVersion(startTs);
        Optional<CommitInfo> maybeCommitInfo = timestampStateStore.getCommitInfo(startTs);

        assertTrue(
                maybeCommitInfo.isPresent() && startVersion.isPresent(),
                "start or commit info not processed for start timestamp");

        CommitInfo commitInfo = maybeCommitInfo.get();

        VersionBounds versionBounds = VersionBounds.builder()
                .startVersion(startVersion)
                .endVersion(commitInfo.commitVersion())
                .build();

        return eventLog.getEventsBetweenVersions(versionBounds).toCommitUpdate(startVersion.get(), commitInfo);
    }

    @Override
    public synchronized TransactionsLockWatchUpdate getUpdateForTransactions(
            Set<Long> startTimestamps, Optional<LockWatchVersion> lastKnownVersion) {
        TimestampMapping timestampMapping = getTimestampMappings(startTimestamps);

        VersionBounds versionBounds = VersionBounds.builder()
                .startVersion(lastKnownVersion)
                .endVersion(timestampMapping.lastVersion())
                .earliestSnapshotVersion(timestampMapping.versionRange().lowerEndpoint())
                .build();

        return eventLog.getEventsBetweenVersions(versionBounds)
                .toTransactionsLockWatchUpdate(timestampMapping, lastKnownVersion);
    }

    @Override
    public synchronized void removeTransactionStateFromCache(long startTimestamp) {
        timestampStateStore.remove(startTimestamp);
        retentionEvents();
    }

    @VisibleForTesting
    synchronized LockWatchEventCacheState getStateForTesting() {
        return ImmutableLockWatchEventCacheState.builder()
                .timestampStoreState(timestampStateStore.getStateForTesting())
                .logState(eventLog.getStateForTesting())
                .build();
    }

    private synchronized TimestampMapping getTimestampMappings(Set<Long> startTimestamps) {
        TimestampMapping.Builder mappingBuilder = new TimestampMapping.Builder();
        startTimestamps.forEach(timestamp -> {
            Optional<LockWatchVersion> entry = timestampStateStore.getStartVersion(timestamp);
            assertTrue(entry.isPresent(), "start timestamp missing from map");
            mappingBuilder.putTimestampMapping(timestamp, entry.get());
        });
        return mappingBuilder.build();
    }

    private synchronized Optional<LockWatchVersion> processEventLogUpdate(LockWatchStateUpdate update) {
        CacheUpdate cacheUpdate = eventLog.processUpdate(update);

        if (cacheUpdate.shouldClearCache()) {
            timestampStateStore.clear();
        }

        retentionEvents();
        return cacheUpdate.getVersion();
    }

    private synchronized void retentionEvents() {
        eventLog.retentionEvents(timestampStateStore.getEarliestLiveSequence());
    }

    private static void assertTrue(boolean condition, String message) {
        if (!condition) {
            throw new TransactionLockWatchFailedException(message);
        }
    }
}
