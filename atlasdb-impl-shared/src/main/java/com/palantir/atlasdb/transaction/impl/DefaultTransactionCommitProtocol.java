/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.transaction.impl;

import java.util.Set;
import java.util.SortedMap;

import com.google.common.collect.Maps;
import com.palantir.atlasdb.transaction.api.TransactionCommitProtocol;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.RemoteLockService;
import com.palantir.timestamp.TimestampService;

public class DefaultTransactionCommitProtocol implements TransactionCommitProtocol {

    private final TimestampService timestampService;
    private final RemoteLockService lockService;

    public DefaultTransactionCommitProtocol(
            TimestampService timestampService,
            RemoteLockService lockService) {
        this.timestampService = timestampService;
        this.lockService = lockService;
    }

    @Override
    public long getCommitTimestamp() {
        return timestampService.getFreshTimestamp();
    }

    @Override
    public LockRefreshToken getCommitLocks(Set<LockDescriptor> lockDescriptors) {
        SortedMap<LockDescriptor, LockMode> locks = buildLockMap(lockDescriptors, LockMode.WRITE);

        return lockAnonymous(LockRequest.builder(locks).build());
    }

    private LockRefreshToken lockAnonymous(LockRequest lockRequest) {
        try {
            return lockService.lock(LockClient.ANONYMOUS.getClientId(), lockRequest);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    private SortedMap<LockDescriptor, LockMode> buildLockMap(Set<LockDescriptor> lockDescriptors, LockMode lockMode) {
        SortedMap<LockDescriptor, LockMode> locks = Maps.newTreeMap();
        for (LockDescriptor descriptor : lockDescriptors) {
            locks.put(descriptor, lockMode);
        }
        return locks;
    }

    @Override
    public void waitForCommitsToComplete(Set<LockDescriptor> lockDescriptors) {
        SortedMap<LockDescriptor, LockMode> locks = buildLockMap(lockDescriptors, LockMode.READ);

        lockAnonymous(LockRequest.builder(locks).lockAndRelease().build());
    }

    @Override
    public void releaseCommitLocks(LockRefreshToken locks) {
        lockService.unlock(locks);
    }

    @Override
    public Set<LockRefreshToken> refreshCommitLocks(Set<LockRefreshToken> locks) {
        return lockService.refreshLockRefreshTokens(locks);
    }

}
