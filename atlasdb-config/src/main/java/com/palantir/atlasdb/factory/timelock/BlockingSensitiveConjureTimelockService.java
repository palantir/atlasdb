/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.factory.timelock;

import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsRequest;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsResponse;
import com.palantir.atlasdb.timelock.api.ConjureLockRequest;
import com.palantir.atlasdb.timelock.api.ConjureLockResponse;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksRequest;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksResponse;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsRequest;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.atlasdb.timelock.api.ConjureTimelockService;
import com.palantir.atlasdb.timelock.api.ConjureUnlockRequest;
import com.palantir.atlasdb.timelock.api.ConjureUnlockResponse;
import com.palantir.atlasdb.timelock.api.ConjureWaitForLocksResponse;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsRequest;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsResponse;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.tokens.auth.AuthHeader;

/**
 * Given two proxies to the same set of underlying TimeLock servers, one configured to expect longer-running operations
 * on the server and one configured not to, routes calls appropriately.
 */
public final class BlockingSensitiveConjureTimelockService implements ConjureTimelockService {
    private final ConjureTimelockService nonblocking;
    private final ConjureTimelockService blocking;

    public BlockingSensitiveConjureTimelockService(
            ConjureTimelockService nonblocking,
            ConjureTimelockService blocking) {
        this.nonblocking = nonblocking;
        this.blocking = blocking;
    }

    @Override
    public ConjureStartTransactionsResponse startTransactions(AuthHeader authHeader, String namespace,
            ConjureStartTransactionsRequest request) {
        return nonblocking.startTransactions(authHeader, namespace, request);
    }

    @Override
    public ConjureGetFreshTimestampsResponse getFreshTimestamps(AuthHeader authHeader, String namespace,
            ConjureGetFreshTimestampsRequest request) {
        return nonblocking.getFreshTimestamps(authHeader, namespace, request);
    }

    @Override
    public LeaderTime leaderTime(AuthHeader authHeader, String namespace) {
        return nonblocking.leaderTime(authHeader, namespace);
    }

    @Override
    public ConjureLockResponse lock(AuthHeader authHeader, String namespace, ConjureLockRequest request) {
        return blocking.lock(authHeader, namespace, request);
    }

    @Override
    public ConjureWaitForLocksResponse waitForLocks(AuthHeader authHeader, String namespace,
            ConjureLockRequest request) {
        return blocking.waitForLocks(authHeader, namespace, request);
    }

    @Override
    public ConjureRefreshLocksResponse refreshLocks(AuthHeader authHeader, String namespace,
            ConjureRefreshLocksRequest request) {
        return nonblocking.refreshLocks(authHeader, namespace, request);
    }

    @Override
    public ConjureUnlockResponse unlock(AuthHeader authHeader, String namespace, ConjureUnlockRequest request) {
        return nonblocking.unlock(authHeader, namespace, request);
    }

    @Override
    public GetCommitTimestampsResponse getCommitTimestamps(AuthHeader authHeader, String namespace,
            GetCommitTimestampsRequest request) {
        return nonblocking.getCommitTimestamps(authHeader, namespace, request);
    }
}
