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

package com.palantir.atlasdb.lock;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.lockwatch.LockWatchState;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.lock.AtlasRowLockDescriptor;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockToken;

public class SimpleLockWatchingResource implements LockWatchingResource {
    private static final TableReference TABLE = TableReference.createFromFullyQualifiedName("test.table");
    private final TransactionManager transactionManager;

    public SimpleLockWatchingResource(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    @Override
    public LockToken lock(String rowName) {
        return transactionManager.getTimelockService().lock(lockRequestForRow(rowName)).getToken();
    }

    @Override
    public void unlock(LockToken token) {
        transactionManager.getTimelockService().unlock(ImmutableSet.of(token));
    }

    @Override
    public void transaction(String rowName) {
        transactionManager.runTaskThrowOnConflict(txn -> {
            txn.put(TABLE, ImmutableMap.of(
                    Cell.create(PtBytes.toBytes(rowName), PtBytes.toBytes("col")), PtBytes.toBytes(123L)));
            txn.commit();
            return null;
        });
    }

    @Override
    public void register(String rowName) {
        transactionManager.getLockWatchingService()
                .registerRowWatches(TABLE, ImmutableSet.of(PtBytes.toBytes(rowName)));
    }

    @Override
    public void deregister(String rowName) {
        transactionManager.getLockWatchingService()
                .deregisterWatches(TABLE, ImmutableSet.of(PtBytes.toBytes(rowName)));
    }

    @Override
    public LockWatchState getWatches() {
        return transactionManager.getLockWatchingService().getLockWatchState();
    }

    public LockDescriptor getLockDescriptor(String row) {
        return AtlasRowLockDescriptor.of(TABLE.getQualifiedName(), PtBytes.toBytes(row));
    }

    private LockRequest lockRequestForRow(String row) {
        return LockRequest.of(ImmutableSet.of(getLockDescriptor(row)), 1000L);
    }
}
