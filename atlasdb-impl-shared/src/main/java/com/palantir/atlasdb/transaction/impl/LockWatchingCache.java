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

package com.palantir.atlasdb.transaction.impl;

import java.util.Map;
import java.util.Set;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.GuardedValue;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockWatch;

public interface LockWatchingCache {
    /**
     * Given a table and a set of cells, the cache returns a map containing the latest cached {@link GuardedValue}s for
     * cells present in the cache.
     *
     * @param tableRef table to read from
     * @param reads set of cells to read
     * @return Cached values that are guaranteed to be the latest visible entries to the transaction making the request
     */
    Map<Cell, GuardedValue> getCached(TableReference tableRef, Set<Cell> reads);

    /**
     * After a transaction has committed, this notifies the cache that the writes can be cached, if desired.
     *
     * @param tableRef the table to which the transaction wrote
     * @param writes actual writes
     * @param lockTimestamp the timestamp of the {@link com.palantir.lock.v2.TimestampedLockToken} from acquiring
     * write locks
     */
    void maybeCacheCommittedWrites(TableReference tableRef, Map<Cell, byte[]> writes, long lockTimestamp);

    /**
     * Only makes sense to actually call with GuardedValues that were committed.
     *
     * @param tableRef table to cache entres for
     * @param writes map detailing values to be cached
     */
    void maybeCacheEntriesRead(TableReference tableRef, Map<Cell, GuardedValue> writes);

    TransactionLockWatchingCacheView getView(long startTimestamp, Map<LockDescriptor, LockWatch> lockWatchState,
            KeyValueService kvs);
}
