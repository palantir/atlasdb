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

import com.google.common.collect.ImmutableSet;

@SuppressWarnings("FinalClass") // mocks
public class NoOpLockWatchEventCache implements LockWatchEventCache {
    public static final LockWatchEventCache INSTANCE = new NoOpLockWatchEventCache();
    private static final TransactionsLockWatchEvents NONE = ImmutableTransactionsLockWatchEvents.builder()
            .clearCache(true)
            .build();

    private NoOpLockWatchEventCache() {
        // singleton
    }

    @Override
    public void removeTimestampFromCache(long timestamp) {

    }

    @Override
    public Optional<IdentifiedVersion> lastKnownVersion() {
        return Optional.empty();
    }

    @Override
    public void processStartTransactionsUpdate(
            Collection<Long> startTimestamps,
            LockWatchStateUpdate update) {
    }

    @Override
    public void processGetCommitTimestampsUpdate(Collection<TransactionUpdate> transactionUpdates,
            LockWatchStateUpdate update) {
    }

    @Override
    public CommitUpdate getCommitUpdate(long _startTs) {
        return CommitUpdate.ignoringWatches();
    }

    @Override
    public TransactionsLockWatchEvents getEventsForTransactions(Set<Long> startTimestamps,
            Optional<IdentifiedVersion> version) {
        return NONE;
    }
}
