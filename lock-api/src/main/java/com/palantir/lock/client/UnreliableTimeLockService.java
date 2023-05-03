/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.buggify.api.BuggifyFactory;
import com.palantir.atlasdb.buggify.impl.DefaultBuggifyFactory;
import com.palantir.lock.v2.ClientLockingOptions;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.timestamp.TimestampRange;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A version of timelock service that has a chance to randomly lose locks during refresh or immediately after
 * acquiring. This is useful for testing the behavior of clients when locks are lost.
 */
public final class UnreliableTimeLockService implements TimelockService {

    private final TimelockService delegate;
    private final BuggifyFactory buggify;

    public static UnreliableTimeLockService create(TimelockService timelockService) {
        return new UnreliableTimeLockService(timelockService, DefaultBuggifyFactory.INSTANCE);
    }

    @VisibleForTesting
    UnreliableTimeLockService(TimelockService delegate, BuggifyFactory buggifyFactory) {
        this.delegate = delegate;
        this.buggify = buggifyFactory;
    }

    @Override
    public boolean isInitialized() {
        return delegate.isInitialized();
    }

    @Override
    public long getFreshTimestamp() {
        return delegate.getFreshTimestamp();
    }

    @Override
    public long getCommitTimestamp(long startTs, LockToken commitLocksToken) {
        return delegate.getCommitTimestamp(startTs, commitLocksToken);
    }

    @Override
    public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        return delegate.getFreshTimestamps(numTimestampsRequested);
    }

    @Override
    public LockImmutableTimestampResponse lockImmutableTimestamp() {
        return delegate.lockImmutableTimestamp();
    }

    @Override
    public List<StartIdentifiedAtlasDbTransactionResponse> startIdentifiedAtlasDbTransactionBatch(int count) {
        return delegate.startIdentifiedAtlasDbTransactionBatch(count);
    }

    @Override
    public long getImmutableTimestamp() {
        return delegate.getImmutableTimestamp();
    }

    @Override
    public LockResponse lock(LockRequest request) {
        LockResponse response = delegate.lock(request);
        buggify.maybe(0.25).run(() -> delegate.unlock(Set.of(response.getToken())));
        return response;
    }

    @Override
    public LockResponse lock(LockRequest lockRequest, ClientLockingOptions options) {
        LockResponse response = delegate.lock(lockRequest, options);
        buggify.maybe(0.25).run(() -> delegate.unlock(Set.of(response.getToken())));
        return response;
    }

    @Override
    public WaitForLocksResponse waitForLocks(WaitForLocksRequest request) {
        return delegate.waitForLocks(request);
    }

    @Override
    public Set<LockToken> refreshLockLeases(Set<LockToken> tokens) {
        Set<LockToken> tokensToRefresh = tokens.stream()
                .filter(_token -> !buggify.maybe(0.25).asBoolean())
                .collect(Collectors.toSet());
        return delegate.refreshLockLeases(tokensToRefresh);
    }

    @Override
    public Set<LockToken> unlock(Set<LockToken> tokens) {
        return delegate.unlock(tokens);
    }

    @Override
    public void tryUnlock(Set<LockToken> tokens) {
        delegate.tryUnlock(tokens);
    }

    @Override
    public long currentTimeMillis() {
        return delegate.currentTimeMillis();
    }
}
