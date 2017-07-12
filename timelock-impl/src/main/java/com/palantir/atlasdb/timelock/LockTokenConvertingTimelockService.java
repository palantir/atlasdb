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

package com.palantir.atlasdb.timelock;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.ws.rs.QueryParam;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.impl.LegacyTimelockService;
import com.palantir.lock.v2.ImmutableLockImmutableTimestampResponse;
import com.palantir.lock.v2.ImmutableLockResponseV2;
import com.palantir.lock.v2.ImmutableLockTokenV2;
import com.palantir.lock.v2.LockImmutableTimestampRequest;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequestV2;
import com.palantir.lock.v2.LockResponseV2;
import com.palantir.lock.v2.LockTokenV2;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.timestamp.TimestampRange;

/**
 * This class handles conversions from LockTokenV2s that may be serializable to the LockRefreshTokenV2Adapters used
 * by the LegacyTimelockService.
 */
public class LockTokenConvertingTimelockService implements TimelockService {
    private static final Logger log = LoggerFactory.getLogger(LockTokenConvertingTimelockService.class);
    private final TimelockService delegate;

    public LockTokenConvertingTimelockService(TimelockService delegate) {
        Preconditions.checkState(delegate instanceof LegacyTimelockService,
                "LockTokenConvertingTimelockService should only be instantiated decorating a LegacyTimelockService");
        this.delegate = delegate;
    }

    @Override
    public long getFreshTimestamp() {
        return delegate.getFreshTimestamp();
    }

    @Override
    public TimestampRange getFreshTimestamps(@QueryParam("number") int numTimestampsRequested) {
        return delegate.getFreshTimestamps(numTimestampsRequested);
    }

    @Override
    public LockImmutableTimestampResponse lockImmutableTimestamp(LockImmutableTimestampRequest request) {
        LockImmutableTimestampResponse delegateResponse = delegate.lockImmutableTimestamp(request);
        return ImmutableLockImmutableTimestampResponse.builder()
                .from(delegateResponse)
                .lock(makeSerializable(delegateResponse.getLock()))
                .build();
    }

    @Override
    public long getImmutableTimestamp() {
        return delegate.getImmutableTimestamp();
    }

    @Override
    public LockResponseV2 lock(LockRequestV2 request) {
        LockResponseV2 delegateResponse = delegate.lock(request);
        if (delegateResponse.wasSuccessful()) {
            return ImmutableLockResponseV2.builder()
                    .from(delegateResponse)
                    .tokenOrEmpty(makeSerializable(delegateResponse.getToken()))
                    .build();
        }
        return ImmutableLockResponseV2.copyOf(delegateResponse);
    }

    @Override
    public WaitForLocksResponse waitForLocks(WaitForLocksRequest request) {
        return delegate.waitForLocks(request);
    }

    @Override
    public Set<LockTokenV2> refreshLockLeases(Set<LockTokenV2> tokens) {
        return makeAllSerializable(delegate.refreshLockLeases(castAllToAdapters(tokens)));
    }

    @Override
    public Set<LockTokenV2> unlock(Set<LockTokenV2> tokens) {
        return makeAllSerializable(delegate.unlock(castAllToAdapters(tokens)));
    }

    @Override
    public long currentTimeMillis() {
        return delegate.currentTimeMillis();
    }

    private static Set<LockTokenV2> makeAllSerializable(Set<LockTokenV2> tokens) {
        return tokens.stream()
                .map(LockTokenConvertingTimelockService::makeSerializable)
                .collect(Collectors.toSet());
    }

    @VisibleForTesting
    static LockTokenV2 makeSerializable(LockTokenV2 token) {
        return ImmutableLockTokenV2.copyOf(token);
    }

    private static Set<LockTokenV2> castAllToAdapters(Set<LockTokenV2> tokens) {
        return tokens.stream()
                .map(LockTokenConvertingTimelockService::castToAdapter)
                .collect(Collectors.toSet());
    }

    @VisibleForTesting
    static LockTokenV2 castToAdapter(LockTokenV2 token) {
        LockRefreshToken legacyToken = new LockRefreshToken(
                calculateLegacyTokenId(token.getRequestId()), Long.MIN_VALUE);
        return new LegacyTimelockService.LockRefreshTokenV2Adapter(legacyToken);
    }

    private static BigInteger calculateLegacyTokenId(UUID uuid) {
        return new BigInteger(ByteBuffer.allocate(16)
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits())
                .array());
    }
}
