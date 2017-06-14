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
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.transaction.api.ImmutableTimestampAndLock;
import com.palantir.atlasdb.transaction.api.TransactionStartProtocol;
import com.palantir.common.base.Throwables;
import com.palantir.lock.AtlasTimestampLockDescriptor;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.RemoteLockService;
import com.palantir.timestamp.TimestampService;

public class DefaultTransactionStartProtocol implements TransactionStartProtocol {

    private final TimestampService timestampService;
    private final RemoteLockService lockService;
    private final LockClient immutableTimestampClient;

    private final AtomicLong recentImmutableTs = new AtomicLong(-1L);

    public DefaultTransactionStartProtocol(
            TimestampService timestampService,
            RemoteLockService lockService,
            LockClient immutableTimestampClient) {
        this.timestampService = timestampService;
        this.lockService = lockService;
        this.immutableTimestampClient = immutableTimestampClient;
    }

    public ImmutableTimestampAndLock getImmutableTimestampAndLock() {
        long immutableLockTs = timestampService.getFreshTimestamp();
        LockDescriptor lockDesc = AtlasTimestampLockDescriptor.of(immutableLockTs);
        LockRequest lockRequest = LockRequest.builder(ImmutableSortedMap.of(lockDesc, LockMode.READ))
                .withLockedInVersionId(immutableLockTs).build();
        LockRefreshToken lock;
        try {
            lock = lockService.lock(immutableTimestampClient.getClientId(), lockRequest);
        } catch (InterruptedException e) {
            throw Throwables.throwUncheckedException(e);
        }
        try {
            return new ImmutableTimestampAndLock(getImmutableTimestampInternal(immutableLockTs), lock);
        } catch (Throwable e) {
            if (lock != null) {
                lockService.unlock(lock);
            }
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

    public long getApproximateImmutableTimestamp() {
        long recentTs = recentImmutableTs.get();
        if (recentTs >= 0) {
            return recentTs;
        }
        return getImmutableTimestamp();
    }

    public long getImmutableTimestamp() {
        long ts = timestampService.getFreshTimestamp();
        return getImmutableTimestampInternal(ts);
    }

    @Override
    public long getStartTimestamp() {
        return timestampService.getFreshTimestamp();
    }

    @Override
    public void releaseImmutableTimestampLock(LockRefreshToken lock) {
        lockService.unlock(lock);
    }

    private SortedMap<LockDescriptor, LockMode> buildLockMap(Set<LockDescriptor> lockDescriptors, LockMode lockMode) {
        SortedMap<LockDescriptor, LockMode> locks = Maps.newTreeMap();
        for (LockDescriptor descriptor : lockDescriptors) {
            locks.put(descriptor, lockMode);
        }
        return locks;
    }

    protected long getImmutableTimestampInternal(long ts) {
        Long minLocked = lockService.getMinLockedInVersionId(immutableTimestampClient.getClientId());
        long ret = minLocked == null ? ts : minLocked;
        long recentTs = recentImmutableTs.get();
        while (recentTs < ret) {
            if (recentImmutableTs.compareAndSet(recentTs, ret)) {
                break;
            } else {
                recentTs = recentImmutableTs.get();
            }
        }
        return ret;
    }
}
