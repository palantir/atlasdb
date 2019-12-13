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

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.client.LeasedLockToken;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.watch.LockWatchInfo;
import com.palantir.lock.watch.LockWatchReferences;
import com.palantir.lock.watch.TableWatchingService;
import com.palantir.lock.watch.TimestampWithLockInfo;

public class SimpleEteLockWatchResource implements EteLockWatchResource {
    private final TransactionManager transactionManager;
    private final TimelockService timelockService;
    private final TableWatchingService tableWatchingService;

    public SimpleEteLockWatchResource(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
        this.timelockService = transactionManager.getTimelockService();
        this.tableWatchingService = transactionManager.getTableWatchingService();
    }

    @Override
    public LockToken lock(LockRequest request) {
        return ((LeasedLockToken) timelockService.lock(request).getToken()).serverToken();
    }

    @Override
    public Set<LockToken> unlock(LockToken token) {
        return timelockService.unlock(ImmutableSet.of(token));
    }

    @Override
    public void registerLockWatch(LockWatchReferences.LockWatchReference request) {
        tableWatchingService.registerWatches(ImmutableSet.of(request));
    }

    @Override
    public long startTransaction() {
        StartIdentifiedAtlasDbTransactionResponse response = timelockService.startIdentifiedAtlasDbTransaction();
        return response.startTimestampAndPartition().timestamp();
    }

    @Override
    public LockWatchInfo getLockWatchInfo(long startTimestamp, LockDescriptor lockDescriptor) {
        return tableWatchingService.getLockWatchState(startTimestamp).lockWatchState(lockDescriptor);
    }

    @Override
    public TimestampWithLockInfo getCommitTimestampAndLockInfo(long startTimestamp, LockToken locksToIgnore) {
        return tableWatchingService.getCommitTimestampWithLockInfo(startTimestamp, locksToIgnore);
    }
}
