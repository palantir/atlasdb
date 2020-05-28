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

package com.palantir.atlasdb.transaction.impl.watch;

import java.util.concurrent.atomic.AtomicReference;

import com.palantir.atlasdb.keyvalue.api.watch.WatchCommitCondition;
import com.palantir.atlasdb.transaction.api.PreCommitCondition;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.lock.watch.CommitUpdate;
import com.palantir.lock.watch.LockWatchEventCache;
import com.palantir.logsafe.Preconditions;

final class WatchPreCommitCondition implements PreCommitCondition {

    private final LockWatchEventCache cache;
    private final WatchCommitCondition watchCommitCondition;
    private AtomicReference<Transaction> txn = new AtomicReference<>(null);

    public WatchPreCommitCondition(LockWatchEventCache cache, WatchCommitCondition watchCommitCondition) {
        this.cache = cache;
        this.watchCommitCondition = watchCommitCondition;
    }

    void initialize(Transaction newTxn) {
        Preconditions.checkState(txn.compareAndSet(null, newTxn), "Already initialized");
    }

    @Override
    public void throwIfConditionInvalid(long timestamp) {
        if (timestamp != getTransaction().getTimestamp()) {
            CommitUpdate commitUpdate = cache.getCommitUpdate(getTransaction().getTimestamp());
            watchCommitCondition.throwIfConflict(commitUpdate);
        }
    }

    @Override
    public void cleanup() {
        cache.removeTransactionStateFromCache(getTransaction().getTimestamp());
    }

    private Transaction getTransaction() {
        return Preconditions.checkNotNull(txn.get(), "Not initialized");
    }
}
