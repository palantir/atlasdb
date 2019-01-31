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

package com.palantir.lock.client;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.palantir.lock.v2.IdentifiedTime;
import com.palantir.lock.v2.IdentifiedTimeLockRequest;
import com.palantir.lock.v2.ImmutableLockImmutableTimestampResponse;
import com.palantir.lock.v2.ImmutableLockResponse;
import com.palantir.lock.v2.ImmutableStartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.LeasableLockResponse;
import com.palantir.lock.v2.LeasableRefreshLockResponse;
import com.palantir.lock.v2.LeasableStartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.Lease;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.StartAtlasDbTransactionResponse;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionRequest;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.timestamp.TimestampRange;

public final class LeasingTimelockClient implements TimelockService {
    private final TimelockRpcClient delegate;

    private LeasingTimelockClient(TimelockRpcClient timelockRpcClient) {
        this.delegate = timelockRpcClient;
    }

    public static LeasingTimelockClient create(TimelockRpcClient timelockRpcClient) {
        return new LeasingTimelockClient(timelockRpcClient);
    }

    @Override
    public long getFreshTimestamp() {
        return delegate.getFreshTimestamp();
    }

    @Override
    public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        return delegate.getFreshTimestamps(numTimestampsRequested);
    }

    @Override
    public LockImmutableTimestampResponse lockImmutableTimestamp(IdentifiedTimeLockRequest request) {
        return delegate.lockImmutableTimestamp(request);
    }

    @Override
    public StartAtlasDbTransactionResponse startAtlasDbTransaction(IdentifiedTimeLockRequest request) {
        return delegate.startAtlasDbTransaction(request);
    }

    @Override
    public StartIdentifiedAtlasDbTransactionResponse startIdentifiedAtlasDbTransaction(
            StartIdentifiedAtlasDbTransactionRequest request) {
        LeasableStartIdentifiedAtlasDbTransactionResponse leasableResponse =
                delegate.leasableStartIdentifiedAtlasDbTransaction(request);

        StartIdentifiedAtlasDbTransactionResponse response = leasableResponse.getStartTransactionResponse();
        Lease lease = leasableResponse.getLease();
        LeasedLockToken leasedLockToken = LeasedLockToken.of(response.immutableTimestamp().getLock(), lease);
        long immutableTs = response.immutableTimestamp().getImmutableTimestamp();

        return ImmutableStartIdentifiedAtlasDbTransactionResponse.of(
                ImmutableLockImmutableTimestampResponse.of(immutableTs, leasedLockToken),
                response.startTimestampAndPartition());
    }

    @Override
    public long getImmutableTimestamp() {
        return delegate.getImmutableTimestamp();
    }

    @Override
    public LockResponse lock(LockRequest request) {
        LeasableLockResponse leasableResponse = delegate.leasableLock(request);

        LockResponse lockResponse = leasableResponse.getLockResponse();
        Lease lease = leasableResponse.getLease();
        LeasedLockToken leasedLockToken = LeasedLockToken.of(lockResponse.getToken(), lease);

        return ImmutableLockResponse.of(Optional.of(leasedLockToken));
    }

    @Override
    public WaitForLocksResponse waitForLocks(WaitForLocksRequest request) {
        return delegate.waitForLocks(request);
    }

    @Override
    public Set<LockToken> refreshLockLeases(Set<LockToken> tokens) {
        IdentifiedTime identifiedTime = delegate.getLeaderTime();
        Set<LeasedLockToken> allTokens = tokens.stream()
                .map(token -> (LeasedLockToken) token)
                .collect(Collectors.toSet());

        Set<LeasedLockToken> validByLease = allTokens.stream()
                .filter(token -> token.isValid(identifiedTime))
                .collect(Collectors.toSet());

        Set<LeasedLockToken> toRefresh = Sets.difference(allTokens, validByLease);

        Set<LeasedLockToken> refreshedTokens = ImmutableSet.of();

        if (!toRefresh.isEmpty()) {
            LeasableRefreshLockResponse refreshLockResponse = delegate.leasableRefreshLockLeases(
                    toRefresh.stream().map(LeasedLockToken::serverToken).collect(Collectors.toSet()));
            Lease lease = refreshLockResponse.getLease();

            refreshedTokens = toRefresh.stream()
                    .filter(t -> refreshLockResponse.refreshedTokens().contains(t.serverToken()))
                    .collect(Collectors.toSet());

            refreshedTokens.forEach(t -> t.updateLease(lease));
        }

        return Sets.union(refreshedTokens, validByLease);
    }

    @Override
    public Set<LockToken> unlock(Set<LockToken> tokens) {
        Set<LeasedLockToken> leasedLockTokens = tokens.stream()
                .map(token -> (LeasedLockToken) token)
                .collect(Collectors.toSet());
        leasedLockTokens.forEach(LeasedLockToken::inValidate);
        return delegate.unlock(leasedLockTokens.stream()
                .map(LeasedLockToken::serverToken)
                .collect(Collectors.toSet()));
    }

    @Override
    public long currentTimeMillis() {
        return delegate.currentTimeMillis();
    }

}
