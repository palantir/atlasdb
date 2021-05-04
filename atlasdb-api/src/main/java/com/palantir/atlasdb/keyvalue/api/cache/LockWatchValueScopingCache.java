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

package com.palantir.atlasdb.keyvalue.api.cache;

import com.palantir.lock.watch.LockWatchValueCache;
import java.util.Set;

/**
 * The idea here is to keep a map of {@link com.palantir.atlasdb.keyvalue.api.CellReference} -> value for each table.
 * Applying lock watch events will:
 *  - for {@link com.palantir.lock.watch.LockEvent}, denote that the given descriptor cannot be cached;
 *  - for {@link com.palantir.lock.watch.UnlockEvent}, denote that the given descriptor can now be cached;
 *  - for {@link com.palantir.lock.watch.LockWatchCreatedEvent}, denote that the given table can now be cached;
 *
 * The central mapping is kept up-to-date with the above, but views of the map are given to transactions which are
 * accurate at the start timestamp's lock watch version. Each transaction then can read from the cache as well as
 * create a digest of values that it has read (and is allowed to read). At commit time, it will flush those values to
 * the central cache (taking in to account the since-locked descriptors), as well as checking for conflicts for
 * serializable transactions by adding a check in the {@link com.palantir.atlasdb.transaction.api.PreCommitCondition}.
 */
public interface LockWatchValueScopingCache extends LockWatchValueCache {
    @Override
    void processStartTransactions(Set<Long> startTimestamps);

    @Override
    void updateCacheAndRemoveTransactionState(long startTimestamp);

    @Override
    void removeTransactionStateFromCache(long startTimestamp);

    TransactionScopedCache createTransactionScopedCache(long startTs);
}
