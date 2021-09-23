/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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
import java.util.Set;
import java.util.stream.Collectors;

public final class LockWatchCacheImpl implements LockWatchCache {
    private final LockWatchEventCache eventCache;
    private final LockWatchValueCache valueCache;

    public LockWatchCacheImpl(LockWatchEventCache eventCache, LockWatchValueCache valueCache) {
        this.eventCache = eventCache;
        this.valueCache = valueCache;
    }

    public static LockWatchCache noOp() {
        return new LockWatchCacheImpl(NoOpLockWatchEventCache.create(), NoOpLockWatchValueCache.create());
    }

    @Override
    public void processStartTransactionsUpdate(Set<Long> startTimestamps, LockWatchStateUpdate update) {
        eventCache.processStartTransactionsUpdate(startTimestamps, update);
        valueCache.processStartTransactions(startTimestamps);
    }

    @Override
    public void processCommitTimestampsUpdate(
            Collection<TransactionUpdate> transactionUpdates, LockWatchStateUpdate update) {
        eventCache.processGetCommitTimestampsUpdate(transactionUpdates, update);
        valueCache.updateCacheWithCommitTimestampsInformation(
                transactionUpdates.stream().map(TransactionUpdate::startTs).collect(Collectors.toSet()));
    }

    @Override
    public void removeTransactionStateFromCache(long startTs) {
        eventCache.removeTransactionStateFromCache(startTs);
        valueCache.ensureStateRemoved(startTs);
    }

    @Override
    public void onSuccess(long startTs) {
        valueCache.onSuccessfulCommit(startTs);
        eventCache.removeTransactionStateFromCache(startTs);
    }

    @Override
    public LockWatchEventCache getEventCache() {
        return eventCache;
    }

    @Override
    public LockWatchValueCache getValueCache() {
        return valueCache;
    }
}
