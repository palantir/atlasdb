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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.watch.TimestampToVersionMap.CommitInfo;
import com.palantir.atlasdb.transaction.api.TransactionLockWatchFailedException;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.ClientLockWatchEventLog;
import com.palantir.lock.watch.ClientLogEvents;
import com.palantir.lock.watch.CommitUpdate;
import com.palantir.lock.watch.IdentifiedVersion;
import com.palantir.lock.watch.ImmutableInvalidateAll;
import com.palantir.lock.watch.ImmutableInvalidateSome;
import com.palantir.lock.watch.LockEvent;
import com.palantir.lock.watch.LockWatchCreatedEvent;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.LockWatchEventCache;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.lock.watch.NoOpLockWatchEventCache;
import com.palantir.lock.watch.TransactionUpdate;
import com.palantir.lock.watch.TransactionsLockWatchEvents;
import com.palantir.lock.watch.UnlockEvent;
import com.palantir.logsafe.Preconditions;

/**
 * This class should only be used through {@link ResilientLockWatchEventCache} as a proxy; failure to do so will result
 * in concurrency issues and inconsistency in the cache state.
 */
public final class LockWatchEventCacheImpl implements LockWatchEventCache {
    private final ClientLockWatchEventLog eventLog;
    private final TimestampToVersionMap timestampMap;

    public static LockWatchEventCache create() {
        return ResilientLockWatchEventCache.newProxyInstance(
                new LockWatchEventCacheImpl(ClientLockWatchEventLogImpl.create()), NoOpLockWatchEventCache.INSTANCE);
    }

    @VisibleForTesting
    LockWatchEventCacheImpl(ClientLockWatchEventLog eventLog) {
        this.eventLog = eventLog;
        timestampMap = new TimestampToVersionMap();
    }

    @Override
    public Optional<IdentifiedVersion> lastKnownVersion() {
        return eventLog.getLatestKnownVersion();
    }

    @Override
    public void processStartTransactionsUpdate(
            Set<Long> startTimestamps,
            LockWatchStateUpdate update) {
        Optional<IdentifiedVersion> latestVersion = processEventLogUpdate(update);

        latestVersion.ifPresent(
                version -> startTimestamps.forEach(timestamp -> timestampMap.putStartVersion(timestamp, version)));

        getEarliestVersion().ifPresent(eventLog::removeOldEntries);
    }

    @Override
    public void processGetCommitTimestampsUpdate(
            Collection<TransactionUpdate> transactionUpdates,
            LockWatchStateUpdate update) {
        Optional<IdentifiedVersion> latestVersion = processEventLogUpdate(update);

        latestVersion.ifPresent(version -> transactionUpdates.forEach(
                transactionUpdate -> checkConditionOrThrow(!timestampMap.putCommitUpdate(transactionUpdate, version),
                        "start timestamp missing from map")));
    }

    @Override
    public CommitUpdate getCommitUpdate(long startTs) {
        Optional<IdentifiedVersion> startVersion = timestampMap.getStartVersion(startTs);
        Optional<CommitInfo> maybeCommitInfo = timestampMap.getCommitInfo(startTs);

        checkConditionOrThrow(!maybeCommitInfo.isPresent() || !startVersion.isPresent(),
                "start or commit info not processed for start timestamp");

        CommitInfo commitInfo = maybeCommitInfo.get();

        ClientLogEvents update = eventLog.getEventsBetweenVersions(startVersion, commitInfo.commitVersion());

        if (update.clearCache()) {
            return ImmutableInvalidateAll.builder().build();
        }

        return constructCommitUpdate(commitInfo, update.events());
    }

    @Override
    public TransactionsLockWatchEvents getEventsForTransactions(
            Set<Long> startTimestamps,
            Optional<IdentifiedVersion> startVersion) {
        Preconditions.checkArgument(!startTimestamps.isEmpty(), "Cannot get events for empty set of transactions");
        Map<Long, IdentifiedVersion> timestampToVersion = getTimestampMappings(startTimestamps);
        IdentifiedVersion endVersion = Collections.max(timestampToVersion.values(), IdentifiedVersion.comparator());
        return eventLog.getEventsBetweenVersions(startVersion, endVersion).map(timestampToVersion);
    }

    @Override
    public void removeTransactionStateFromCache(long startTimestamp) {
        timestampMap.remove(startTimestamp);
    }

    @VisibleForTesting
    Map<Long, IdentifiedVersion> getTimestampMappings(Set<Long> startTimestamps) {
        Map<Long, IdentifiedVersion> timestampToVersion = new HashMap<>();
        startTimestamps.forEach(timestamp -> {
            Optional<IdentifiedVersion> entry = timestampMap.getStartVersion(timestamp);
            checkConditionOrThrow(!entry.isPresent(), "start timestamp missing from map");
            timestampToVersion.put(timestamp, entry.get());
        });
        return timestampToVersion;
    }

    @VisibleForTesting
    Optional<IdentifiedVersion> getEarliestVersion() {
        Optional<IdentifiedVersion> currentVersion = eventLog.getLatestKnownVersion();
        return timestampMap.getEarliestVersion().flatMap(sequence ->
                currentVersion.map(version -> IdentifiedVersion.of(version.id(), sequence)));
    }

    private void checkConditionOrThrow(boolean condition, String message) {
        if (condition) {
            throw new TransactionLockWatchFailedException(message);
        }
    }

    private CommitUpdate constructCommitUpdate(CommitInfo commitInfo, List<LockWatchEvent> events) {
        LockEventVisitor eventVisitor = new LockEventVisitor(commitInfo.commitLockToken());
        Set<LockDescriptor> locksTakenOut = new HashSet<>();
        events.forEach(event -> locksTakenOut.addAll(event.accept(eventVisitor)));
        return ImmutableInvalidateSome.builder().invalidatedLocks(locksTakenOut).build();
    }

    private Optional<IdentifiedVersion> processEventLogUpdate(LockWatchStateUpdate update) {
        Optional<IdentifiedVersion> currentVersion = eventLog.getLatestKnownVersion();
        Optional<IdentifiedVersion> latestVersion = eventLog.processUpdate(update);

        if (!(latestVersion.isPresent()
                && currentVersion.isPresent()
                && latestVersion.get().id().equals(currentVersion.get().id())
                && update.accept(SuccessVisitor.INSTANCE))) {
            timestampMap.clear();
        }
        return latestVersion;
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
}
