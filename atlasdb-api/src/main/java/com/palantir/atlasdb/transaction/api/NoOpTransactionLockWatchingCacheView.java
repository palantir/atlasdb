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

public final class NoOpTransactionLockWatchingCacheView implements TransactionLockWatchingCacheView {
    public static final NoOpTransactionLockWatchingCacheView INSTANCE = new NoOpTransactionLockWatchingCacheView();

    private NoOpTransactionLockWatchingCacheView() {
        // use reflection (please don't)
    }

    @Override
    public Map<Cell, byte[]> readCached(TableReference tableRef, Set<Cell> cells) {
        return ImmutableMap.of();
    }

    @Override
    public void tryCacheNewValuesRead(TableReference tableRef, Map<Cell, byte[]> writes, LockWatchStateUpdate lwState) {
        // noop
    }

    @Override
    public void tryCacheWrittenValues(TableReference tableRef, Map<Cell, byte[]> writes, long lockTs) {
        // noop
    }
}
