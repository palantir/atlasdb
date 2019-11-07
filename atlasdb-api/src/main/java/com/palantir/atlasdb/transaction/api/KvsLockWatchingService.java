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

import java.util.Set;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.annotation.Idempotent;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.watch.LockWatchState;

public interface KvsLockWatchingService {
    /**
     * Registers watches for a set of rows in a single table.
     * @param tableRef table to register watches for.
     * @param rowNames rows to register watches for.
     * @return a mapping of {@link com.palantir.lock.LockDescriptor}s to their corresponding {@link RowReference} for
     * which the watches have been registered.
     */
    @Idempotent
    RowLockDescriptorMapping registerRowWatches(TableReference tableRef, Set<byte[]> rowNames);

    /**
     * Deregisters watches for a set of rows in a single table, if they exist.
     * @param tableRef table to deregister watches for.
     * @param rowNames rows to deregister watches for.
     * @return the set of  {@link com.palantir.lock.LockDescriptor}s corresponding to the removed watches
     */
    Set<LockDescriptor> deregisterRowWatches(TableReference tableRef, Set<byte[]> rowNames);

    /**
     * Returns the current state of all registered watches.
     * @return a mapping of {@link com.palantir.lock.LockDescriptor}s to their corresponding
     * {@link com.palantir.lock.watch.LockWatch}.
     */
    LockWatchState getLockWatchState();
}
