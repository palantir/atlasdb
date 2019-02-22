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

package com.palantir.lock.v2;

import java.util.Set;
import java.util.UUID;

import com.palantir.lock.client.IdentifiedLockRequest;
import com.palantir.timestamp.TimestampRange;

public final class BridgingTimelockService implements TimelockService {
    private final TimelockRpcClient delegate;
    private final UUID clientId;

    private BridgingTimelockService(TimelockRpcClient timelockRpcClient, UUID clientId) {
        this.delegate = timelockRpcClient;
        this.clientId = clientId;
    }

    public static TimelockService create(TimelockRpcClient timelockRpcClient) {
        return new BridgingTimelockService(timelockRpcClient, UUID.randomUUID());
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
    public LockImmutableTimestampResponse lockImmutableTimestamp() {
        return delegate.lockImmutableTimestamp(IdentifiedTimeLockRequest.create());
    }

    @Override
    public StartIdentifiedAtlasDbTransactionResponse startIdentifiedAtlasDbTransaction() {
        return delegate.startAtlasDbTransaction(
                ImmutableStartIdentifiedAtlasDbTransactionRequest.of(UUID.randomUUID(), clientId))
                .toStartTransactionResponse();
    }

    @Override
    public long getImmutableTimestamp() {
        return delegate.getImmutableTimestamp();
    }

    @Override
    public LockResponse lock(LockRequest request) {
        return delegate.lock(IdentifiedLockRequest.from(request)).accept(LockResponseV2.Visitor.of(
                successful -> LockResponse.successful(successful.getToken()),
                unsuccessful -> LockResponse.timedOut()));
    }

    @Override
    public WaitForLocksResponse waitForLocks(WaitForLocksRequest request) {
        return delegate.waitForLocks(request);
    }

    @Override
    public Set<LockToken> refreshLockLeases(Set<LockToken> tokens) {
        return delegate.refreshLockLeases(tokens).refreshedTokens();
    }

    @Override
    public Set<LockToken> unlock(Set<LockToken> tokens) {
        return delegate.unlock(tokens);
    }

    @Override
    public long currentTimeMillis() {
        return delegate.currentTimeMillis();
    }
}
