/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.timelock.paxos;

import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.timelock.AsyncTimelockService;
import com.palantir.lock.client.IdentifiedLockRequest;
import com.palantir.lock.v2.ClientLockingOptions;
import com.palantir.lock.v2.IdentifiedTimeLockRequest;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockResponseV2;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.RefreshLockResponseV2;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.timestamp.TimestampRange;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

final class DelegatingTimelockService implements TimelockService {
    private final AsyncTimelockService timelock;

    public DelegatingTimelockService(AsyncTimelockService timelock) {
        this.timelock = timelock;
    }

    @Override
    public long getFreshTimestamp() {
        return timelock.getFreshTimestamp();
    }

    @Override
    public long getCommitTimestamp(long _startTs, LockToken _commitLocksToken) {
        return getFreshTimestamp();
    }

    @Override
    public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        return timelock.getFreshTimestamps(numTimestampsRequested);
    }

    @Override
    public LockImmutableTimestampResponse lockImmutableTimestamp() {
        return timelock.lockImmutableTimestamp(IdentifiedTimeLockRequest.create());
    }

    @Override
    public long getImmutableTimestamp() {
        return timelock.getImmutableTimestamp();
    }

    @Override
    public LockResponse lock(LockRequest request) {
        LockResponseV2 lockResponseV2 = tryGet(timelock.lock(IdentifiedLockRequest.from(request)));
        return lockResponseV2.accept(new LockResponseV2.Visitor<>() {
            @Override
            public LockResponse visit(LockResponseV2.Successful successful) {
                return LockResponse.successful(successful.getToken());
            }

            @Override
            public LockResponse visit(LockResponseV2.Unsuccessful failure) {
                return LockResponse.timedOut();
            }
        });
    }

    @Override
    public LockResponse lock(LockRequest lockRequest, ClientLockingOptions options) {
        return lock(lockRequest);
    }

    @Override
    public WaitForLocksResponse waitForLocks(WaitForLocksRequest request) {
        return tryGet(timelock.waitForLocks(request));
    }

    @Override
    public Set<LockToken> refreshLockLeases(Set<LockToken> tokens) {
        ListenableFuture<RefreshLockResponseV2> future = timelock.refreshLockLeases(tokens);
        return tryGet(future).refreshedTokens();
    }

    @Override
    public Set<LockToken> unlock(Set<LockToken> tokens) {
        return tryGet(timelock.unlock(tokens));
    }

    @Override
    public void tryUnlock(Set<LockToken> tokens) {
        // TODO(gs): swallow exceptions?
        unlock(tokens);
    }

    @Override
    public long currentTimeMillis() {
        return timelock.currentTimeMillis();
    }

    private <T> T tryGet(ListenableFuture<T> future) {
        try {
            return future.get(5L, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new SafeRuntimeException("Async request failed", e);
        }
    }
}
