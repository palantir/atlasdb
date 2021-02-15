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
     * Try to cache values read from the kvs in this transaction. It is the responsibility of the underlying
     * {@link LockWatchingCache} to arbitrate which, if any, values should be cached.
     *
     * @param tableRef table we read from
     * @param writes entries read from the KVS
     * @param lwState lock watch state when the view was created
     */
    void tryCacheNewValuesRead(TableReference tableRef, Map<Cell, byte[]> writes, LockWatchStateUpdate lwState);

    /**
     * Try to cache values written to the kvs in this transaction. This method must be called only after the writing
     * transaction has successfully committed.
     *
     * It is the responsibility of the underlying {@link LockWatchingCache} to arbitrate which, if any, values should be
     * cached.
     *
     * @param tableRef table we wrote to
     * @param writes entries written to the KVS
     * @param lockTs lock timestamp of the {@link com.palantir.lock.watch.TimestampedLockResponse} from acquiring locks
     */
    void tryCacheWrittenValues(TableReference tableRef, Map<Cell, byte[]> writes, long lockTs);
}
