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

import java.util.Collection;
import java.util.Optional;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.keyvalue.api.watch.TimestampStateStore.CommitInfo;
import com.palantir.atlasdb.transaction.api.TransactionLockWatchFailedException;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.lock.watch.CommitUpdate;
import com.palantir.lock.watch.LockWatchEventCache;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.lock.watch.NoOpLockWatchEventCache;
import com.palantir.lock.watch.TransactionUpdate;
import com.palantir.lock.watch.TransactionsLockWatchUpdate;
import com.palantir.logsafe.Preconditions;

/**
 * This class should only be used through {@link ResilientLockWatchEventCache} as a proxy; failure to do so will result
 * in concurrency issues and inconsistency in the cache state.
 */
public final class LockWatchEventCacheImpl implements LockWatchEventCache {
    // This value should be the same as in TimeLock's LockEventLogImpl.
    private static final int MAX_EVENTS = 1000;

    private final LockWatchEventLog eventLog;
    private final TimestampStateStore timestampStateStore;

    public static LockWatchEventCache create(MetricsManager metricsManager) {
        return ResilientLockWatchEventCache.newProxyInstance(
                new LockWatchEventCacheImpl(LockWatchEventLog.create(MAX_EVENTS)), NoOpLockWatchEventCache.INSTANCE,
                metricsManager);
    }

    @VisibleForTesting
    LockWatchEventCacheImpl(LockWatchEventLog eventLog) {
        this.eventLog = eventLog;
        timestampStateStore = new TimestampStateStore();
    }

    @Override
    public Optional<LockWatchVersion> lastKnownVersion() {
        return eventLog.getLatestKnownVersion();
    }

    @Override
    public void processStartTransactionsUpdate(
            Set<Long> startTimestamps,
            LockWatchStateUpdate update) {
        Optional<LockWatchVersion> updateVersion = processEventLogUpdate(update);
        updateVersion.ifPresent(version -> timestampStateStore.putStartTimestamps(startTimestamps, version));
    }

    @Override
    public void processGetCommitTimestampsUpdate(
            Collection<TransactionUpdate> transactionUpdates,
            LockWatchStateUpdate update) {
        Optional<LockWatchVersion> updateVersion = processEventLogUpdate(update);
        updateVersion.ifPresent(version -> timestampStateStore.putCommitUpdates(transactionUpdates, version));
    }

    @Override
    public CommitUpdate getCommitUpdate(long startTs) {
        Optional<LockWatchVersion> startVersion = timestampStateStore.getStartVersion(startTs);
        Optional<CommitInfo> maybeCommitInfo = timestampStateStore.getCommitInfo(startTs);

        assertTrue(maybeCommitInfo.isPresent() && startVersion.isPresent(),
                "start or commit info not processed for start timestamp");

        CommitInfo commitInfo = maybeCommitInfo.get();
        return eventLog.getEventsBetweenVersions(startVersion, commitInfo.commitVersion())
                .toCommitUpdate(startVersion.get(), commitInfo);
    }

    @Override
    public TransactionsLockWatchUpdate getUpdateForTransactions(
            Set<Long> startTimestamps,
            Optional<LockWatchVersion> lastKnownVersion) {
        Preconditions.checkArgument(!startTimestamps.isEmpty(), "Cannot get events for empty set of transactions");
        TimestampMapping timestampMapping = getTimestampMappings(startTimestamps);

        return eventLog.getEventsBetweenVersions(lastKnownVersion, timestampMapping.lastVersion())
                .toTransactionsLockWatchUpdate(timestampMapping, lastKnownVersion);
    }

    @Override
    public void removeTransactionStateFromCache(long startTimestamp) {
        timestampStateStore.remove(startTimestamp);
    }

    @VisibleForTesting
    LockWatchEventCacheState getStateForTesting() {
        return ImmutableLockWatchEventCacheState.builder()
                .timestampStoreState(timestampStateStore.getStateForTesting())
                .logState(eventLog.getStateForTesting())
                .build();
    }

    private TimestampMapping getTimestampMappings(Set<Long> startTimestamps) {
        TimestampMapping.Builder mappingBuilder = new TimestampMapping.Builder();
        startTimestamps.forEach(timestamp -> {
            Optional<LockWatchVersion> entry = timestampStateStore.getStartVersion(timestamp);
            assertTrue(entry.isPresent(), "start timestamp missing from map");
            mappingBuilder.putTimestampMapping(timestamp, entry.get());
        });
        return mappingBuilder.build();
    }

    private Optional<LockWatchVersion> processEventLogUpdate(LockWatchStateUpdate update) {
        CacheUpdate cacheUpdate = eventLog.processUpdate(update);

        if (cacheUpdate.shouldClearCache()) {
            timestampStateStore.clear();
        }

        eventLog.retentionEvents();

        return cacheUpdate.getVersion();
    }

    private static void assertTrue(boolean condition, String message) {
        if (!condition) {
            throw new TransactionLockWatchFailedException(message);
        }
    }
}
