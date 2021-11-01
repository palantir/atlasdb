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

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.lock.watch.LockWatchStateUpdate;
import java.util.Map;
import java.util.Set;

public final class NoOpLockWatchingCache implements LockWatchingCache {
    public static final NoOpLockWatchingCache INSTANCE = new NoOpLockWatchingCache();

    private NoOpLockWatchingCache() {
        // you wanted to be tricky?
    }

    @Override
    public Map<Cell, GuardedValue> getCached(TableReference _tableRef, Set<Cell> _reads) {
        return ImmutableMap.of();
    }

    @Override
    public void maybeCacheCommittedWrites(TableReference _tableRef, Map<Cell, byte[]> _writes) {
        // noop
    }

    @Override
    public void maybeCacheEntriesRead(
            TableReference _tableRef, Map<Cell, byte[]> _writes, LockWatchStateUpdate _lwState) {
        // noop
    }

    @Override
    public TransactionLockWatchingCacheView getView(long _startTimestamp, LockWatchStateUpdate _lockWatchState) {
        return NoOpTransactionLockWatchingCacheView.INSTANCE;
    }
}
