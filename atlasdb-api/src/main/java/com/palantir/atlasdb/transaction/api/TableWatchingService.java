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

import java.util.Optional;

import com.palantir.common.annotation.Idempotent;
import com.palantir.lock.watch.LockWatchInfo;
import com.palantir.lock.watch.WatchId;

public interface TableWatchingService {
    /**
     * Registers watches for a set of rows in a single table.
     * @param tableRef table to register watches for.
     * @param rowNames rows to register watches for.
     * @return a mapping of {@link WatchId}s to the {@link RowOrCellReference} for which the watches have been registered
     */
    @Idempotent
    void registerWatches(TableLockWatchEntry lockWatchEntries);

    /**
     * Deregisters watches for a set of rows in a single table, if they exist.
     * @param tableRef table to deregister watches for.
     * @param rowNames rows to deregister watches for.
     * @return the set of {@link WatchId}s corresponding to the removed watches
     */
    void deregisterWatches(TableLockWatchEntry lockWatchEntries);

    /**
     * Returns the current state of all registered watches.
     * @return a mapping of {@link WatchId}s to their corresponding {@link LockWatchInfo}
     */
    TableWatchState getLockWatchState(Optional<Long> lastKnownState);
}
