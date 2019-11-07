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

import java.util.Map;
import java.util.Set;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;

// todo (gmaretic): decide if the logic for what can be cached should live here or in the cache itself
public interface TransactionLockWatchingCacheView {
    /**
     * Attempts to read the cached versions of cells. If there is no suitable cached value for the cell, it will not be
     * present in the returned map.
     *
     * @param tableRef table to read from
     * @param cells cells to read
     * @return a mapping with all cells that were cached and guaranteed to be equal to what the transaction would have
     * read from the kvs.
     */
    Map<Cell, byte[]> readCached(TableReference tableRef, Set<Cell> cells);

    /**
     * Try to cache values read from the kvs in this transaction. It is the responsibility of the caller to ensure the
     * values can be cached (the corresponding locks were not open)
     *
     * @param tableRef table we read from
     * @param writes a mapping of cells to guarded values (guarded by the start timestamp) to be cached
     */
    void tryCacheNewValuesRead(TableReference tableRef, Map<Cell, GuardedValue> writes);

    /**
     * Try to cache values written to the kvs in this transaction. It is the responsibility of the caller to ensure
     * this is done only after the transaction has committed.
     *
     * @param tableRef table we wrote to
     * @param writes writes
     * @param lockTimestamp lock timestamp of the {@link com.palantir.lock.watch.TimestampedLockResponse} from
     * acquiring write locks.
     */
    void tryCacheWrittenValues(TableReference tableRef, Map<Cell, byte[]> writes, long lockTimestamp);
}
