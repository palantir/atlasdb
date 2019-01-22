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
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import com.palantir.lock.v2.ContractedLockResponse;
import com.palantir.lock.v2.ContractedRefreshLockResponse;
import com.palantir.lock.v2.ContractedStartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.IdentifiedTimeLockRequest;
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

public class LeasingTimelockClient implements TimelockService {
    private TimelockServerInterface delegate;
    private LockLeaseManager lockLeaseManager;

    private LeasingTimelockClient(TimelockServerInterface timelockService, LockLeaseManager lockLeaseManager) {
        this.delegate = timelockService;
        this.lockLeaseManager = lockLeaseManager;
    }

    public static LeasingTimelockClient create(TimelockServerInterface timelockServerInterface) {
        return new LeasingTimelockClient(timelockServerInterface, LockLeaseManager.create());
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
        long startTime = System.nanoTime();
        ContractedStartIdentifiedAtlasDbTransactionResponse contractedResponse =
                delegate.contractedStartIdentifiedAtlasDbTransaction(request);

        StartIdentifiedAtlasDbTransactionResponse response = contractedResponse.getStartTransactionResponse();
        Optional<Duration> leasePeriod = contractedResponse.getLeasePeriod();

        leasePeriod.ifPresent(period -> lockLeaseManager.updateLease(
                response.immutableTimestamp().getLock(),
                startTime + period.toNanos()));

        return response;
    }

    @Override
    public long getImmutableTimestamp() {
        return delegate.getImmutableTimestamp();
    }

    @Override
    public LockResponse lock(LockRequest request) {
        long startTime = System.nanoTime();
        ContractedLockResponse contractedResponse = delegate.contractedLock(request);

        LockResponse lockResponse = contractedResponse.getLockResponse();
        Optional<Duration> leasePeriod = contractedResponse.getLeasePeriod();

        if (lockResponse.wasSuccessful()) {
            leasePeriod.ifPresent(period -> lockLeaseManager.updateLease(
                    lockResponse.getToken(),
                    startTime + period.toNanos()));
        }

        return lockResponse;
    }

    @Override
    public WaitForLocksResponse waitForLocks(WaitForLocksRequest request) {
        return delegate.waitForLocks(request);
    }

    @Override
    public Set<LockToken> refreshLockLeases(Set<LockToken> tokens) {
        Set<LockToken> validByLease = tokens.stream()
                .filter(lockLeaseManager::isValid)
                .collect(Collectors.toSet());

        Set<LockToken> toRefresh = Sets.difference(tokens, validByLease);

        long startTime = System.nanoTime();
        ContractedRefreshLockResponse refreshLockResponse = delegate.contractedRefreshLockLeases(toRefresh);

        Set<LockToken> refreshed = refreshLockResponse.refreshedTokens();
        Optional<Duration> leasePeriod = refreshLockResponse.getLeasePeriod();

        leasePeriod.ifPresent(period -> refreshed.forEach(lockToken ->
            lockLeaseManager.updateLease(lockToken, startTime + period.toNanos())));

        //register refreshed tokens to lock lease manager, probably it is better to have a bulk method on manager.
        return Sets.union(refreshed, validByLease);
    }

    @Override
    public Set<LockToken> unlock(Set<LockToken> tokens) {
        tokens.forEach(lockLeaseManager::invalidate);
        return delegate.unlock(tokens);
    }

    @Override
    public long currentTimeMillis() {
        return 0;
    }
}
