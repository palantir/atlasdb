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

package com.palantir.lock.impl;

import java.util.Set;
import java.util.SortedMap;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.palantir.common.base.Throwables;
import com.palantir.lock.AtlasTimestampLockDescriptor;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequestV2;
import com.palantir.lock.v2.TimelockService;
import com.palantir.timestamp.TimestampRange;
import com.palantir.timestamp.TimestampService;

public class LegacyTimelockService implements TimelockService {

    private final TimestampService timestampService;
    private final RemoteLockService lockService;
    private final LockClient immutableTimestampClient;

    public LegacyTimelockService(TimestampService timestampService, RemoteLockService lockService,
            LockClient immutableTimestampClient) {
        this.timestampService = timestampService;
        this.lockService = lockService;
        this.immutableTimestampClient = immutableTimestampClient;
    }

    @Override
    public long getFreshTimestamp() {
        return timestampService.getFreshTimestamp();
    }

    @Override
    public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        return timestampService.getFreshTimestamps(numTimestampsRequested);
    }

    @Override
    public LockImmutableTimestampResponse lockImmutableTimestamp() {
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
            return new LockImmutableTimestampResponse(getImmutableTimestampInternal(immutableLockTs), lock);
        } catch (Throwable e) {
            if (lock != null) {
                lockService.unlock(lock);
            }
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

    @Override
    public long getImmutableTimestamp() {
        long ts = timestampService.getFreshTimestamp();
        return getImmutableTimestampInternal(ts);
    }

    @Override
    public LockRefreshToken lock(LockRequestV2 request) {
        LockRequest legacyRequest = toLegacyLockRequest(request);

        return lockAnonymous(legacyRequest);
    }

    @Override
    public void waitForLocks(Set<LockDescriptor> lockDescriptors) {
        LockRequest legacyRequest = toLegacyWaitForLocksRequest(lockDescriptors);

        lockAnonymous(legacyRequest);
    }

    private LockRequest toLegacyLockRequest(LockRequestV2 request) {
       SortedMap<LockDescriptor, LockMode> locks = buildLockMap(request.getLockDescriptors(), LockMode.WRITE);
       return LockRequest.builder(locks).build();
    }

    private LockRequest toLegacyWaitForLocksRequest(Set<LockDescriptor> lockDescriptors) {
        SortedMap<LockDescriptor, LockMode> locks = buildLockMap(lockDescriptors, LockMode.READ);
        return LockRequest.builder(locks).lockAndRelease().build();
    }

    @Override
    public Set<LockRefreshToken> refreshLockLeases(Set<LockRefreshToken> tokens) {
        return lockService.refreshLockRefreshTokens(tokens);
    }

    @Override
    public Set<LockRefreshToken> unlock(Set<LockRefreshToken> tokens) {
        Set<LockRefreshToken> unlocked = Sets.newHashSet();
        for (LockRefreshToken token : tokens) {
            if (lockService.unlock(token)) {
                unlocked.add(token);
            }
        }
        return unlocked;
    }

    @Override
    public long currentTimeMillis() {
        return lockService.currentTimeMillis();
    }

    protected long getImmutableTimestampInternal(long ts) {
        Long minLocked = lockService.getMinLockedInVersionId(immutableTimestampClient.getClientId());
        return minLocked == null ? ts : minLocked;
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
}
