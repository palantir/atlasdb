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

import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampResponse;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsRequest;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsResponse;
import com.palantir.atlasdb.timelock.api.ConjureLockRequest;
import com.palantir.atlasdb.timelock.api.ConjureLockResponse;
import com.palantir.atlasdb.timelock.api.ConjureLockResponseV2;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksRequest;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksRequestV2;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksResponse;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksResponseV2;
import com.palantir.atlasdb.timelock.api.ConjureStartOneTransactionRequest;
import com.palantir.atlasdb.timelock.api.ConjureStartOneTransactionResponse;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsRequest;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.atlasdb.timelock.api.ConjureTimelockService;
import com.palantir.atlasdb.timelock.api.ConjureUnlockRequest;
import com.palantir.atlasdb.timelock.api.ConjureUnlockRequestV2;
import com.palantir.atlasdb.timelock.api.ConjureUnlockResponse;
import com.palantir.atlasdb.timelock.api.ConjureUnlockResponseV2;
import com.palantir.atlasdb.timelock.api.ConjureWaitForLocksResponse;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsRequest;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsResponse;
import com.palantir.atlasdb.timelock.api.GetOneCommitTimestampRequest;
import com.palantir.atlasdb.timelock.api.GetOneCommitTimestampResponse;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.tokens.auth.AuthHeader;
import java.io.InputStream;
import javax.ws.rs.core.StreamingOutput;

/**
 * Given two proxies to the same set of underlying TimeLock servers, one configured to expect longer-running operations
 * on the server and one configured not to, routes calls appropriately.
 */
public final class TimeoutSensitiveConjureTimelockService implements ConjureTimelockService {
    private final ConjureTimelockService longTimeoutProxy;
    private final ConjureTimelockService shortTimeoutProxy;

    public TimeoutSensitiveConjureTimelockService(
            ShortAndLongTimeoutServices<ConjureTimelockService> conjureTimelockServices) {
        this.longTimeoutProxy = conjureTimelockServices.longTimeout();
        this.shortTimeoutProxy = conjureTimelockServices.shortTimeout();
    }

    @Override
    public ConjureStartTransactionsResponse startTransactions(
            AuthHeader authHeader, String namespace, ConjureStartTransactionsRequest request) {
        return shortTimeoutProxy.startTransactions(authHeader, namespace, request);
    }

    @Override
    public ConjureGetFreshTimestampsResponse getFreshTimestamps(
            AuthHeader authHeader, String namespace, ConjureGetFreshTimestampsRequest request) {
        return shortTimeoutProxy.getFreshTimestamps(authHeader, namespace, request);
    }

    @Override
    public ConjureStartOneTransactionResponse startOneTransaction(
            AuthHeader authHeader, String namespace, ConjureStartOneTransactionRequest request) {
        return shortTimeoutProxy.startOneTransaction(authHeader, namespace, request);
    }

    @Override
    public ConjureGetFreshTimestampResponse getFreshTimestamp(AuthHeader authHeader, String namespace) {
        return shortTimeoutProxy.getFreshTimestamp(authHeader, namespace);
    }

    @Override
    public LeaderTime leaderTime(AuthHeader authHeader, String namespace) {
        return shortTimeoutProxy.leaderTime(authHeader, namespace);
    }

    @Override
    public ConjureLockResponse lock(AuthHeader authHeader, String namespace, ConjureLockRequest request) {
        return longTimeoutProxy.lock(authHeader, namespace, request);
    }

    @Override
    public ConjureLockResponseV2 lockV2(AuthHeader authHeader, String namespace, ConjureLockRequest request) {
        return longTimeoutProxy.lockV2(authHeader, namespace, request);
    }

    @Override
    public ConjureWaitForLocksResponse waitForLocks(
            AuthHeader authHeader, String namespace, ConjureLockRequest request) {
        return longTimeoutProxy.waitForLocks(authHeader, namespace, request);
    }

    @Override
    public ConjureRefreshLocksResponse refreshLocks(
            AuthHeader authHeader, String namespace, ConjureRefreshLocksRequest request) {
        return shortTimeoutProxy.refreshLocks(authHeader, namespace, request);
    }

    @Override
    public ConjureRefreshLocksResponseV2 refreshLocksV2(
            AuthHeader authHeader, String namespace, ConjureRefreshLocksRequestV2 request) {
        return shortTimeoutProxy.refreshLocksV2(authHeader, namespace, request);
    }

    @Override
    public ConjureUnlockResponse unlock(AuthHeader authHeader, String namespace, ConjureUnlockRequest request) {
        return shortTimeoutProxy.unlock(authHeader, namespace, request);
    }

    @Override
    public ConjureUnlockResponseV2 unlockV2(AuthHeader authHeader, String namespace, ConjureUnlockRequestV2 request) {
        return shortTimeoutProxy.unlockV2(authHeader, namespace, request);
    }

    @Override
    public GetCommitTimestampsResponse getCommitTimestamps(
            AuthHeader authHeader, String namespace, GetCommitTimestampsRequest request) {
        return shortTimeoutProxy.getCommitTimestamps(authHeader, namespace, request);
    }

    @Override
    public GetOneCommitTimestampResponse getOneCommitTimestamp(
            AuthHeader authHeader, String namespace, GetOneCommitTimestampRequest request) {
        return shortTimeoutProxy.getOneCommitTimestamp(authHeader, namespace, request);
    }

    @Override
    public StreamingOutput runCommands(AuthHeader authHeader, InputStream requests) {
        // The locking endpoints can't be batched in runCommands anyway
        return shortTimeoutProxy.runCommands(authHeader, requests);
    }
}
