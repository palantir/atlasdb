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

package com.palantir.atlasdb.transaction.api;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.lock.watch.LockWatchStateUpdate;
import java.util.Map;
import java.util.Set;

public interface LockWatchingCache {
    /**
     * Given a table and a set of cells, the cache returns a map containing the latest cached {@link GuardedValue}s for
     * cells present in the cache.
     *
     * @param tableRef table to read from
     * @param reads set of cells to read
     * @return Cached values that are guaranteed to be the latest visible entries to the transaction making the request
     * if the {@link GuardedValue}'s timestamp matches the transaction's lock watch state
     */
    Map<Cell, GuardedValue> getCached(TableReference tableRef, Set<Cell> reads);

    /**
     * After a transaction has committed, this notifies the cache that the writes can be cached, if desired.
     *
     * @param tableRef the table to which the transaction wrote
     * @param writes actual writes
     */
    void maybeCacheCommittedWrites(TableReference tableRef, Map<Cell, byte[]> writes);

    /**
     * A transaction can attempt to cache entries read during the transaction using this method. The implementation of
     * the {@link LockWatchStateUpdate} must correctly arbitrate which of the passed entries are safe to cache.
     *
     * @param tableRef table to cache entries for
     * @param writes entries read by the transaction
     * @param lockWatchState lock watch state at the start of the transaction that read the entries
     */
    void maybeCacheEntriesRead(TableReference tableRef, Map<Cell, byte[]> writes, LockWatchStateUpdate lockWatchState);

    /**
     * Creates a view of the cache for a transaction, based on the start timestamp and the lock watch state.
     *
     * @param startTimestamp of the transaction
     * @param lockWatchState returned from the {@link com.palantir.lock.v2.StartTransactionWithWatchesResponse}
     * @return view of the cache
     */
    TransactionLockWatchingCacheView getView(long startTimestamp, LockWatchStateUpdate lockWatchState);
}
