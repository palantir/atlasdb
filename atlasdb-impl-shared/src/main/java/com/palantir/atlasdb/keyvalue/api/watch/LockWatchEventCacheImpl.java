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
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.watch.TimestampStateStore.CommitInfo;
import com.palantir.atlasdb.transaction.api.TransactionLockWatchFailedException;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.CommitUpdate;
import com.palantir.lock.watch.ImmutableInvalidateAll;
import com.palantir.lock.watch.ImmutableInvalidateSome;
import com.palantir.lock.watch.LockEvent;
import com.palantir.lock.watch.LockWatchCreatedEvent;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.LockWatchEventCache;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.lock.watch.NoOpLockWatchEventCache;
import com.palantir.lock.watch.TransactionUpdate;
import com.palantir.lock.watch.TransactionsLockWatchUpdate;
import com.palantir.lock.watch.UnlockEvent;
import com.palantir.logsafe.Preconditions;

/**
 * This class should only be used through {@link ResilientLockWatchEventCache} as a proxy; failure to do so will result
 * in concurrency issues and inconsistency in the cache state.
 */
public final class LockWatchEventCacheImpl implements LockWatchEventCache {
    /**
     * This value should be at least as large as the max size in LockEventLogImpl. If it is smaller, it is possible that
     * new events are immediately deleted.
     */
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

        ClientLogEvents update = eventLog.getEventsBetweenVersions(startVersion, commitInfo.commitVersion());

        if (update.clearCache()) {
            return ImmutableInvalidateAll.builder().build();
        }

        return createCommitUpdate(commitInfo, update.events().events());
    }

    @Override
    public TransactionsLockWatchUpdate getUpdateForTransactions(
            Set<Long> startTimestamps,
            Optional<LockWatchVersion> lastKnownVersion) {
        Preconditions.checkArgument(!startTimestamps.isEmpty(), "Cannot get events for empty set of transactions");
        TimestampMapping timestampMapping = getTimestampMappings(startTimestamps);
        LockWatchVersion endVersion = timestampMapping.maxVersion();
        ClientLogEvents events = eventLog.getEventsBetweenVersions(lastKnownVersion, endVersion);
        assertEventsStillPresentForTimestamps(timestampMapping, events);

        return eventLog.getEventsBetweenVersions(lastKnownVersion, endVersion).map(timestampMapping.timestampMapping());
    }

    @Override
    public void removeTransactionStateFromCache(long startTimestamp) {
        timestampStateStore.remove(startTimestamp);
    }

    @VisibleForTesting
    TimestampMapping getTimestampMappings(Set<Long> startTimestamps) {
        TimestampMapping.Builder mappingBuilder = new TimestampMapping.Builder();
        startTimestamps.forEach(timestamp -> {
            Optional<LockWatchVersion> entry = timestampStateStore.getStartVersion(timestamp);
            assertTrue(entry.isPresent(), "start timestamp missing from map");
            mappingBuilder.putTimestampMapping(timestamp, entry.get());
        });
        return mappingBuilder.build();
    }

    @VisibleForTesting
    LockWatchEventCacheState getStateForTesting() {
        return ImmutableLockWatchEventCacheState.builder()
                .timestampStoreState(timestampStateStore.getStateForTesting())
                .logState(eventLog.getStateForTesting())
                .build();
    }


    private Optional<LockWatchVersion> processEventLogUpdate(LockWatchStateUpdate update) {
        CacheUpdate cacheUpdate = eventLog.processUpdate(update);

        if (cacheUpdate.shouldClearCache()) {
            timestampStateStore.clear();
        }

        eventLog.retentionEvents();

        return cacheUpdate.getVersion();
    }

    private static CommitUpdate createCommitUpdate(CommitInfo commitInfo, List<LockWatchEvent> events) {
        LockEventVisitor eventVisitor = new LockEventVisitor(commitInfo.commitLockToken());
        Set<LockDescriptor> locksTakenOut = new HashSet<>();
        events.forEach(event -> locksTakenOut.addAll(event.accept(eventVisitor)));
        return ImmutableInvalidateSome.builder().invalidatedLocks(locksTakenOut).build();
    }

    private static void assertTrue(boolean condition, String message) {
        if (!condition) {
            throw new TransactionLockWatchFailedException(message);
        }
    }

    private static void assertEventsStillPresentForTimestamps(
            TimestampMapping timestampMapping,
            ClientLogEvents events) {
        events.events().firstVersion().ifPresent(
                minVersion -> assertTrue(minVersion <= timestampMapping.minVersion().version(),
                        "Earliest version in the timestamp mapping has already been deleted from the event cache"));
        events.events().lastVersion().ifPresent(
                maxVersion -> assertTrue(maxVersion >= timestampMapping.maxVersion().version(),
                        "Latest version in the timestamp mapping has already been deleted from the event cache"));
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
}
