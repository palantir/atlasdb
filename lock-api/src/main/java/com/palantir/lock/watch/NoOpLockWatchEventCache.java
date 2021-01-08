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
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@SuppressWarnings("FinalClass") // mocks
public class NoOpLockWatchEventCache implements LockWatchEventCache {
    private static final LockWatchVersion FAKE_VERSION = LockWatchVersion.of(UUID.randomUUID(), -1L);
    private Optional<LockWatchVersion> currentVersion = Optional.empty();

    private NoOpLockWatchEventCache() {}

    public static LockWatchEventCache create() {
        return new NoOpLockWatchEventCache();
    }

    @Override
    public boolean isEnabled() {
        return false;
    }

    @Override
    public Optional<LockWatchVersion> lastKnownVersion() {
        return currentVersion;
    }

    @Override
    public void processStartTransactionsUpdate(Set<Long> startTimestamps, LockWatchStateUpdate update) {
        updateVersion(extractVersionFromUpdate(update));
    }

    @Override
    public void processGetCommitTimestampsUpdate(
            Collection<TransactionUpdate> transactionUpdates, LockWatchStateUpdate update) {
        updateVersion(extractVersionFromUpdate(update));
    }

    @Override
    public CommitUpdate getCommitUpdate(long startTs) {
        return ImmutableInvalidateAll.builder().build();
    }

    @Override
    public TransactionsLockWatchUpdate getUpdateForTransactions(
            Set<Long> startTimestamps, Optional<LockWatchVersion> version) {
        return ImmutableTransactionsLockWatchUpdate.builder()
                .clearCache(true)
                .startTsToSequence(startTimestamps.stream()
                        .collect(Collectors.toMap(startTs -> startTs, $ -> currentVersion.orElse(FAKE_VERSION))))
                .build();
    }

    @Override
    public void removeTransactionStateFromCache(long startTimestamp) {}

    private void updateVersion(Optional<LockWatchVersion> maybeNewVersion) {
        currentVersion = maybeNewVersion.map(newVersion -> currentVersion
                .map(current -> {
                    if (current.id().equals(newVersion.id()) && current.version() > newVersion.version()) {
                        return current;
                    } else {
                        return newVersion;
                    }
                })
                .orElse(newVersion));
    }

    /**
     * This method mirrors the way in which we extract versions from state updates in the real implementation of
     * {@link LockWatchEventCache}. Notably, while a success update does contain all the necessary information to
     * construct an update, we cannot use that update if the leader has changed - we must have a snapshot after a leader
     * change. Thus, this method may return empty even though there is a version on the update (and this is the only
     * case in which an empty may be returned).
     */
    private Optional<LockWatchVersion> extractVersionFromUpdate(LockWatchStateUpdate update) {
        return update.accept(new LockWatchStateUpdate.Visitor<Optional<LockWatchVersion>>() {

            @Override
            public Optional<LockWatchVersion> visit(LockWatchStateUpdate.Success success) {
                return currentVersion
                        .filter(current -> current.id().equals(success.logId()))
                        .map(_unused -> LockWatchVersion.of(success.logId(), success.lastKnownVersion()));
            }

            @Override
            public Optional<LockWatchVersion> visit(LockWatchStateUpdate.Snapshot snapshot) {
                return Optional.of(LockWatchVersion.of(snapshot.logId(), snapshot.lastKnownVersion()));
            }
        });
    }
}
