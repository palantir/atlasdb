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
    public static final LockWatchEventCache INSTANCE = new NoOpLockWatchEventCache();
    private static final LockWatchVersion FAKE_VERSION = LockWatchVersion.of(UUID.randomUUID(), -1L);

    private NoOpLockWatchEventCache() {
        // singleton
    }

    @Override
    public Optional<LockWatchVersion> lastKnownVersion() {
        return Optional.empty();
    }

    @Override
    public void processStartTransactionsUpdate(Set<Long> startTimestamps, LockWatchStateUpdate update) {
    }

    @Override
    public void processGetCommitTimestampsUpdate(Collection<TransactionUpdate> transactionUpdates,
            LockWatchStateUpdate update) {
    }

    @Override
    public CommitUpdate getCommitUpdate(long startTs) {
        return ImmutableInvalidateAll.builder().build();
    }

    @Override
    public TransactionsLockWatchUpdate getUpdateForTransactions(Set<Long> startTimestamps,
            Optional<LockWatchVersion> version) {
        return ImmutableTransactionsLockWatchUpdate.builder()
                .clearCache(true)
                .startTsToSequence(
                        startTimestamps.stream().collect(Collectors.toMap(startTs -> startTs, $ -> FAKE_VERSION)))
                .build();
    }

    @Override
    public void removeTransactionStateFromCache(long startTimestamp) {
    }
}
