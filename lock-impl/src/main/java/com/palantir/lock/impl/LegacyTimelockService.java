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

import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.palantir.common.base.Throwables;
import com.palantir.lock.AtlasTimestampLockDescriptor;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.SimpleTimeDuration;
import com.palantir.lock.v2.LockImmutableTimestampRequest;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.timestamp.TimestampRange;
import com.palantir.timestamp.TimestampService;

/**
 * A {@link TimelockService} implementation that delegates to a {@link RemoteLockService} and {@link TimestampService}.
 */
public class LegacyTimelockService implements TimelockService {

    private final TimestampService timestampService;
    private final RemoteLockService lockService;
    private final LockClient immutableTsLockClient;

    public LegacyTimelockService(TimestampService timestampService, RemoteLockService lockService,
            LockClient immutableTsLockClient) {
        this.timestampService = timestampService;
        this.lockService = lockService;
        this.immutableTsLockClient = immutableTsLockClient;
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
    public LockImmutableTimestampResponse lockImmutableTimestamp(LockImmutableTimestampRequest request) {
        long immutableLockTs = timestampService.getFreshTimestamp();
        LockDescriptor lockDesc = AtlasTimestampLockDescriptor.of(immutableLockTs);
        com.palantir.lock.LockRequest lockRequest = com.palantir.lock.LockRequest.builder(
                ImmutableSortedMap.of(lockDesc, LockMode.READ))
                .withLockedInVersionId(immutableLockTs).build();
        LockRefreshToken lock;

        try {
            lock = lockService.lock(immutableTsLockClient.getClientId(), lockRequest);
        } catch (InterruptedException e) {
            throw Throwables.throwUncheckedException(e);
        }

        try {
            return LockImmutableTimestampResponse.of(
                    getImmutableTimestampInternal(immutableLockTs),
                    new LockRefreshTokenV2Adapter(lock));
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
    public LockResponse lock(LockRequest request) {
        LockRefreshToken legacyToken = lockAnonymous(toLegacyLockRequest(request));
        if (legacyToken == null) {
            return LockResponse.timedOut();
        } else {
            return LockResponse.successful(new LockRefreshTokenV2Adapter(legacyToken));
        }
    }

    @Override
    public WaitForLocksResponse waitForLocks(WaitForLocksRequest request) {
        com.palantir.lock.LockRequest legacyRequest = toLegacyWaitForLocksRequest(request.getLockDescriptors());

        // this blocks indefinitely, and can only fail if the connection fails (and throws an exception)
        lockAnonymous(legacyRequest);
        return WaitForLocksResponse.successful();
    }

    private com.palantir.lock.LockRequest toLegacyLockRequest(LockRequest request) {
        SortedMap<LockDescriptor, LockMode> locks = buildLockMap(request.getLockDescriptors(), LockMode.WRITE);
        return com.palantir.lock.LockRequest.builder(locks)
                .blockForAtMost(SimpleTimeDuration.of(request.getAcquireTimeoutMs(), TimeUnit.MILLISECONDS))
                .build();
    }

    private com.palantir.lock.LockRequest toLegacyWaitForLocksRequest(Set<LockDescriptor> lockDescriptors) {
        SortedMap<LockDescriptor, LockMode> locks = buildLockMap(lockDescriptors, LockMode.READ);
        return com.palantir.lock.LockRequest.builder(locks).lockAndRelease().build();
    }

    @Override
    public Set<LockToken> refreshLockLeases(Set<LockToken> tokens) {
        Set<LockRefreshToken> refreshTokens = tokens.stream()
                .map(this::getRefreshToken)
                .collect(Collectors.toSet());
        return lockService.refreshLockRefreshTokens(refreshTokens).stream()
                .map(LockRefreshTokenV2Adapter::new)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<LockToken> unlock(Set<LockToken> tokens) {
        Set<LockToken> unlocked = Sets.newHashSet();
        for (LockToken tokenV2 : tokens) {
            if (lockService.unlock(getRefreshToken(tokenV2))) {
                unlocked.add(tokenV2);
            }
        }
        return unlocked;
    }

    private LockRefreshToken getRefreshToken(LockToken tokenV2) {
        Preconditions.checkArgument(
                (tokenV2 instanceof LockRefreshTokenV2Adapter),
                "The LockToken instance passed to LegacyTimelockService was of an unexpected type. "
                        + "LegacyTimelockService only supports operations on the tokens it returns.");
        return ((LockRefreshTokenV2Adapter) tokenV2).getToken();
    }

    @Override
    public long currentTimeMillis() {
        return lockService.currentTimeMillis();
    }

    private long getImmutableTimestampInternal(long ts) {
        Long minLocked = lockService.getMinLockedInVersionId(immutableTsLockClient.getClientId());
        return minLocked == null ? ts : minLocked;
    }

    private LockRefreshToken lockAnonymous(com.palantir.lock.LockRequest lockRequest) {
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

    public static class LockRefreshTokenV2Adapter implements LockToken {

        private final LockRefreshToken token;
        private final UUID requestId;

        public LockRefreshTokenV2Adapter(LockRefreshToken token) {
            this.token = token;
            this.requestId = getRequestId(token);
        }

        @Override
        public UUID getRequestId() {
            return requestId;
        }

        public LockRefreshToken getToken() {
            return token;
        }

        private static UUID getRequestId(LockRefreshToken token) {
            long msb = token.getTokenId().shiftRight(64).longValue();
            long lsb = token.getTokenId().longValue();
            return new UUID(msb, lsb);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            LockRefreshTokenV2Adapter that = (LockRefreshTokenV2Adapter) o;
            return Objects.equals(token, that.token) &&
                    Objects.equals(requestId, that.requestId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(token, requestId);
        }
    }
}
