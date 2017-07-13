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
import com.palantir.lock.v2.ImmutableLockToken;
import com.palantir.lock.v2.LockImmutableTimestampRequest;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
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
    public LockResponse lock(LockRequest request) {
        LockResponse delegateResponse = delegate.lock(request);
        if (delegateResponse.wasSuccessful()) {
            return ImmutableLockResponseV2.builder()
                    .from(delegateResponse)
                    .tokenOrEmpty(makeSerializable(delegateResponse.getToken()))
                    .build();
        }
        return delegateResponse;
    }

    @Override
    public WaitForLocksResponse waitForLocks(WaitForLocksRequest request) {
        return delegate.waitForLocks(request);
    }

    @Override
    public Set<LockToken> refreshLockLeases(Set<LockToken> tokens) {
        return makeAllSerializable(delegate.refreshLockLeases(castAllToAdapters(tokens)));
    }

    @Override
    public Set<LockToken> unlock(Set<LockToken> tokens) {
        return makeAllSerializable(delegate.unlock(castAllToAdapters(tokens)));
    }

    @Override
    public long currentTimeMillis() {
        return delegate.currentTimeMillis();
    }

    private static Set<LockToken> makeAllSerializable(Set<LockToken> tokens) {
        return tokens.stream()
                .map(LockTokenConvertingTimelockService::makeSerializable)
                .collect(Collectors.toSet());
    }

    @VisibleForTesting
    static LockToken makeSerializable(LockToken token) {
        return ImmutableLockToken.copyOf(token);
    }

    private static Set<LockToken> castAllToAdapters(Set<LockToken> tokens) {
        return tokens.stream()
                .map(LockTokenConvertingTimelockService::castToAdapter)
                .collect(Collectors.toSet());
    }

    @VisibleForTesting
    static LockToken castToAdapter(LockToken token) {
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
