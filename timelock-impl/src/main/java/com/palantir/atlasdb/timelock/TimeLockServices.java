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
package com.palantir.atlasdb.timelock;

import java.math.BigInteger;
import java.util.Set;

import org.immutables.value.Value;

import com.google.common.util.concurrent.Futures;
import com.palantir.lock.HeldLocksGrant;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockResponse;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.LockService;
import com.palantir.lock.SimpleHeldLocksToken;
import com.palantir.lock.impl.AsyncLockService;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampRange;
import com.palantir.timestamp.TimestampService;

@Value.Immutable
public interface TimeLockServices {
    static TimeLockServices create(
            AsyncLockService lockService,
            AsyncTimelockService timelockService,
            AsyncTimelockResource timelockResource) {
        return ImmutableTimeLockServices.builder()
                .asyncLockService(lockService)
                .timelockService(timelockService)
                .timelockResource(timelockResource)
                .build();
    }

    // The Jersey endpoints
    AsyncTimelockResource getTimelockResource();
    // The RPC-independent leadership-enabled implementation of the timelock service
    AsyncTimelockService getTimelockService();

    // Async lock service
    AsyncLockService getAsyncLockService();

    @Deprecated
    @Value.Derived
    default LockService getLockService() {
        return new LockService() {
            @Override
            public LockResponse lockWithFullLockResponse(LockClient client, LockRequest request) {
                return Futures.getUnchecked(getAsyncLockService().lockWithFullLockResponse(client, request));
            }

            @Override
            public boolean unlock(HeldLocksToken token) {
                return Futures.getUnchecked(getAsyncLockService().unlock(token));
            }

            @Override
            public boolean unlockSimple(SimpleHeldLocksToken token) {
                return Futures.getUnchecked(getAsyncLockService().unlockSimple(token));
            }

            @Override
            public boolean unlockAndFreeze(HeldLocksToken token) {
                return Futures.getUnchecked(getAsyncLockService().unlockAndFreeze(token));
            }

            @Override
            public Set<HeldLocksToken> getTokens(LockClient client) {
                return Futures.getUnchecked(getAsyncLockService().getTokens(client));
            }

            @Override
            public Set<HeldLocksToken> refreshTokens(Iterable<HeldLocksToken> tokens) {
                return Futures.getUnchecked(getAsyncLockService().refreshTokens(tokens));
            }

            @Override
            public HeldLocksGrant refreshGrant(HeldLocksGrant grant) {
                return Futures.getUnchecked(getAsyncLockService().refreshGrant(grant));
            }

            @Override
            public HeldLocksGrant refreshGrant(BigInteger grantId) {
                return Futures.getUnchecked(getAsyncLockService().refreshGrant(grantId));
            }

            @Override
            public HeldLocksGrant convertToGrant(HeldLocksToken token) {
                return Futures.getUnchecked(getAsyncLockService().convertToGrant(token));
            }

            @Override
            public HeldLocksToken useGrant(LockClient client, HeldLocksGrant grant) {
                return Futures.getUnchecked(getAsyncLockService().useGrant(client, grant));
            }

            @Override
            public HeldLocksToken useGrant(LockClient client, BigInteger grantId) {
                return Futures.getUnchecked(getAsyncLockService().useGrant(client, grantId));
            }

            @Override
            public Long getMinLockedInVersionId() {
                return Futures.getUnchecked(getAsyncLockService().getMinLockedInVersionId());
            }

            @Override
            public Long getMinLockedInVersionId(LockClient client) {
                return Futures.getUnchecked(getAsyncLockService().getMinLockedInVersionId(client));
            }

            @Override
            public LockServerOptions getLockServerOptions() {
                return Futures.getUnchecked(getAsyncLockService().getLockServerOptions());
            }

            @Override
            public long currentTimeMillis() {
                return Futures.getUnchecked(getAsyncLockService().currentTimeMillis());
            }

            @Override
            public void logCurrentState() {
                Futures.getUnchecked(getAsyncLockService().logCurrentState());
            }

            @Override
            public LockRefreshToken lock(String client, LockRequest request) {
                return Futures.getUnchecked(getAsyncLockService().lock(client, request));
            }

            @Override
            public HeldLocksToken lockAndGetHeldLocks(String client, LockRequest request) {
                return Futures.getUnchecked(getAsyncLockService().lockAndGetHeldLocks(client, request));
            }

            @Override
            public boolean unlock(LockRefreshToken token) {
                return Futures.getUnchecked(getAsyncLockService().unlock(token));
            }

            @Override
            public Set<LockRefreshToken> refreshLockRefreshTokens(Iterable<LockRefreshToken> tokens) {
                return Futures.getUnchecked(getAsyncLockService().refreshLockRefreshTokens(tokens));
            }

            @Override
            public Long getMinLockedInVersionId(String client) {
                return Futures.getUnchecked(getAsyncLockService().getMinLockedInVersionId(client));
            }
        };
    }

    @Deprecated
    @Value.Derived
    default TimestampService getTimestampService() {
        return new TimestampService() {
            @Override
            public long getFreshTimestamp() {
                return Futures.getUnchecked(getTimelockService().getFreshTimestamp());
            }

            @Override
            public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
                return Futures.getUnchecked(getTimelockService().getFreshTimestamps(numTimestampsRequested));
            }
        };
    }

    @Deprecated
    @Value.Derived
    default TimestampManagementService getTimestampManagementService() {
        return new TimestampManagementService() {
            @Override
            public void fastForwardTimestamp(long currentTimestamp) {
                Futures.getUnchecked(getTimelockService().fastForwardTimestamp(currentTimestamp));
            }

            @Override
            public String ping() {
                return Futures.getUnchecked(getTimelockService().ping());
            }
        };
    }
}
