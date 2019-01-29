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

import java.time.Duration;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.Sets;
import com.palantir.common.time.NanoTime;
import com.palantir.lock.v2.IdentifiedTimeLockRequest;
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
    private final LockLeaseManager lockLeaseManager;

    private LeasingTimelockClient(TimelockRpcClient timelockService, LockLeaseManager lockLeaseManager) {
        this.delegate = timelockService;
        this.lockLeaseManager = lockLeaseManager;
    }

    public static LeasingTimelockClient create(TimelockRpcClient timelockRpcClient) {
        return new LeasingTimelockClient(timelockRpcClient, LockLeaseManager.create(timelockRpcClient::getLeaderTime));
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
        Optional<Lease> lease = leasableResponse.getLease();

        lease.ifPresent(l -> updateLockLeases(response.immutableTimestamp().getLock(), l.startTime(), l.period()));

        return response;
    }

    @Override
    public long getImmutableTimestamp() {
        return delegate.getImmutableTimestamp();
    }

    @Override
    public LockResponse lock(LockRequest request) {
        LeasableLockResponse leasableResponse = delegate.leasableLock(request);

        LockResponse lockResponse = leasableResponse.getLockResponse();
        Optional<Lease> optionalLease = leasableResponse.getLease();

        if (lockResponse.wasSuccessful() && optionalLease.isPresent()) {
            Lease lease = optionalLease.get();
            updateLockLeases(lockResponse.getToken(), lease.startTime(), lease.period());
        }

        return lockResponse;
    }

    @Override
    public WaitForLocksResponse waitForLocks(WaitForLocksRequest request) {
        return delegate.waitForLocks(request);
    }

    @Override
    public Set<LockToken> refreshLockLeases(Set<LockToken> tokens) {
        Set<LockToken> validByLease = lockLeaseManager.isValid(tokens);

        Set<LockToken> toRefresh = Sets.difference(tokens, validByLease);

        LeasableRefreshLockResponse refreshLockResponse = delegate.leasableRefreshLockLeases(toRefresh);

        Set<LockToken> refreshed = refreshLockResponse.refreshedTokens();
        Optional<Lease> lease = refreshLockResponse.getLease();

        lease.ifPresent(l -> updateLockLeases(refreshed, l.startTime(), l.period()));

        return Sets.union(refreshed, validByLease);
    }

    @Override
    public Set<LockToken> unlock(Set<LockToken> tokens) {
        tokens.forEach(lockLeaseManager::invalidate);
        return delegate.unlock(tokens);
    }

    @Override
    public long currentTimeMillis() {
        return delegate.currentTimeMillis();
    }

    private void updateLockLeases(LockToken token, NanoTime startTimeNanos, Duration leasePeriod) {
        lockLeaseManager.updateLease(token,startTimeNanos.plus(leasePeriod));
    }

    private void updateLockLeases(Set<LockToken> tokens, NanoTime startTimeNanos, Duration leasePeriod) {
        tokens.forEach(lockToken -> updateLockLeases(lockToken, startTimeNanos, leasePeriod));
    }
}
