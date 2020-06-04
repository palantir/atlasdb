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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.TimestampToVersionMap.CommitInfo;
import com.palantir.lock.watch.TimestampToVersionMap.MapEntry;
import com.palantir.logsafe.Preconditions;

/**
 * This class should only be used through {@link FailureCheckingLockWatchEventCache} as a proxy; failure to do so will
 * result in concurrency issues and inconsistency in the cache state.
 */
public final class LockWatchEventCacheImpl implements LockWatchEventCache {
    private final ClientLockWatchEventLog eventLog;
    private final TimestampToVersionMap timestampMap;

    public static LockWatchEventCache create() {
        return FailureCheckingLockWatchEventCache.newProxyInstance(
                new LockWatchEventCacheImpl(ClientLockWatchEventLogImpl.create()));
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

        latestVersion.ifPresent(version -> startTimestamps.forEach(timestamp -> timestampMap.putStartVersion(timestamp, version)));

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
        Optional<MapEntry> maybeEntry = timestampMap.get(startTs);
        Optional<CommitInfo> maybeCommitInfo = maybeEntry.flatMap(MapEntry::commitInfo);

        checkConditionOrThrow(!maybeCommitInfo.isPresent(), "commit info not processed for start timestamp");

        CommitInfo commitInfo = maybeCommitInfo.get();

        ClientLogEvents update =
                eventLog.getEventsBetweenVersions(Optional.of(maybeEntry.get().version()), commitInfo.commitVersion());

        if (update.clearCache()) {
            return ImmutableInvalidateAll.builder().build();
        }

        return constructCommitUpdate(commitInfo, update.events());
    }

    @Override
    public TransactionsLockWatchEvents getEventsForTransactions(
            Set<Long> startTimestamps,
            Optional<IdentifiedVersion> startVersion) {
        Preconditions.checkArgument(!startTimestamps.isEmpty(), "Cannot get events for empty set of tranasctions");
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
            Optional<MapEntry> entry = timestampMap.get(timestamp);
            checkConditionOrThrow(!entry.isPresent(), "start timestamp missing from map");
            timestampToVersion.put(timestamp, entry.get().version());
        });
        return timestampToVersion;
    }

    @VisibleForTesting
    Optional<IdentifiedVersion> getEarliestVersion() {
        return timestampMap.getEarliestVersion();
    }

    private void checkConditionOrThrow(boolean condition, String message) {
        if (condition) {
            throw new LockWatchFailedException(message);
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
