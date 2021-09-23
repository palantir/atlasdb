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

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.lock.watch.*;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;

class DuplicatingLockWatchEventCache implements LockWatchEventCache {
    private final LockWatchEventCache mainCache;
    private final LockWatchEventCache secondaryCache;

    DuplicatingLockWatchEventCache(LockWatchEventCache mainCache, LockWatchEventCache secondaryCache) {
        this.mainCache = mainCache;
        this.secondaryCache = secondaryCache;
    }

    @Override
    public boolean isEnabled() {
        return mainCache.isEnabled();
    }

    @Override
    public Optional<LockWatchVersion> lastKnownVersion() {
        return mainCache.lastKnownVersion();
    }

    @Override
    public void processStartTransactionsUpdate(Set<Long> startTimestamps, LockWatchStateUpdate update) {
        mainCache.processStartTransactionsUpdate(startTimestamps, update);
        secondaryCache.processStartTransactionsUpdate(startTimestamps, update);
        validateVersionEquality();
    }

    @Override
    public void processGetCommitTimestampsUpdate(
            Collection<TransactionUpdate> transactionUpdates, LockWatchStateUpdate update) {
        mainCache.processGetCommitTimestampsUpdate(transactionUpdates, update);
        secondaryCache.processGetCommitTimestampsUpdate(transactionUpdates, update);
        validateVersionEquality();
    }

    @Override
    public CommitUpdate getCommitUpdate(long startTs) {
        return mainCache.getCommitUpdate(startTs);
    }

    @Override
    public TransactionsLockWatchUpdate getUpdateForTransactions(
            Set<Long> startTimestamps, Optional<LockWatchVersion> version) {
        return mainCache.getUpdateForTransactions(startTimestamps, version);
    }

    @Override
    public void removeTransactionStateFromCache(long startTimestamp) {
        mainCache.removeTransactionStateFromCache(startTimestamp);
    }

    @Override
    public CommitUpdate getEventUpdate(long startTs) {
        return mainCache.getEventUpdate(startTs);
    }

    private void validateVersionEquality() {
        assertThat(mainCache.lastKnownVersion()).isEqualTo(secondaryCache.lastKnownVersion());
    }
}
