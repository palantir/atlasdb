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

package com.palantir.atlasdb.keyvalue.api.watch;

import com.palantir.atlasdb.keyvalue.api.cache.TransactionScopedCache;
import com.palantir.lock.watch.CommitUpdate;
import com.palantir.lock.watch.LockWatchCache;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.lock.watch.TransactionsLockWatchUpdate;
import java.util.Optional;
import java.util.Set;

public abstract class LockWatchManagerInternal extends LockWatchManager implements AutoCloseable {
    public abstract LockWatchCache getCache();

    public abstract void updateCacheAndRemoveTransactionState(long startTs);

    public abstract void removeTransactionStateFromCache(long startTs);

    public abstract TransactionScopedCache createTransactionScopedCache(long startTs);

    @Override
    public abstract CommitUpdate getCommitUpdate(long startTs);

    @Override
    public abstract TransactionsLockWatchUpdate getUpdateForTransactions(
            Set<Long> startTimestamps, Optional<LockWatchVersion> version);

    @Override
    public abstract void close();
}
