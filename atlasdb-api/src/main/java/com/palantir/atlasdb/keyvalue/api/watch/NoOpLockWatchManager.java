/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

import com.palantir.atlasdb.keyvalue.api.cache.NoOpTransactionScopedCache;
import com.palantir.atlasdb.keyvalue.api.cache.TransactionScopedCache;
import com.palantir.lock.watch.*;
import java.util.Optional;
import java.util.Set;

public final class NoOpLockWatchManager extends LockWatchManagerInternal {
    private final LockWatchCache cache;

    private NoOpLockWatchManager(LockWatchCache cache) {
        this.cache = cache;
    }

    public static LockWatchManagerInternal create() {
        return new NoOpLockWatchManager(LockWatchCacheImpl.noOp());
    }

    @Override
    public void registerPreciselyWatches(Set<LockWatchReferences.LockWatchReference> lockWatchReferences) {
        // Ignored
    }

    @Override
    boolean isEnabled() {
        return cache.getEventCache().isEnabled();
    }

    @Override
    CommitUpdate getCommitUpdate(long startTs) {
        return cache.getEventCache().getCommitUpdate(startTs);
    }

    @Override
    public LockWatchCache getCache() {
        return cache;
    }

    @Override
    public void removeTransactionStateFromCache(long startTs) {
        cache.removeTransactionStateFromCache(startTs);
    }

    @Override
    public void onSuccess(long startTs) {
        cache.onSuccess(startTs);
    }

    @Override
    TransactionsLockWatchUpdate getUpdateForTransactions(
            Set<Long> startTimestamps, Optional<LockWatchVersion> version) {
        return cache.getEventCache().getUpdateForTransactions(startTimestamps, version);
    }

    @Override
    public TransactionScopedCache getTransactionScopedCache(long startTs) {
        return NoOpTransactionScopedCache.create();
    }

    @Override
    public TransactionScopedCache getReadOnlyTransactionScopedCache(long startTs) {
        return NoOpTransactionScopedCache.create().createReadOnlyCache(CommitUpdate.invalidateAll());
    }

    @Override
    public void close() {
        // cool story
    }
}
