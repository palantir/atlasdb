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

import com.palantir.atlasdb.timelock.api.AcquireNamedMinimumTimestampLeaseRequest;
import com.palantir.atlasdb.timelock.api.AcquireNamedMinimumTimestampLeaseResponse;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsRequest;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsRequestV2;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsResponse;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsResponseV2;
import com.palantir.atlasdb.timelock.api.ConjureLockRequest;
import com.palantir.atlasdb.timelock.api.ConjureLockResponse;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksRequest;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksRequestV2;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksResponse;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksResponseV2;
import com.palantir.atlasdb.timelock.api.ConjureSingleTimestamp;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsRequest;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.atlasdb.timelock.api.ConjureTimelockService;
import com.palantir.atlasdb.timelock.api.ConjureUnlockRequest;
import com.palantir.atlasdb.timelock.api.ConjureUnlockRequestV2;
import com.palantir.atlasdb.timelock.api.ConjureUnlockResponse;
import com.palantir.atlasdb.timelock.api.ConjureUnlockResponseV2;
import com.palantir.atlasdb.timelock.api.ConjureWaitForLocksResponse;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampRequest;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampResponse;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsRequest;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsResponse;
import com.palantir.atlasdb.timelock.api.NamedMinimumTimestampLessor;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.tokens.auth.AuthHeader;

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
    public ConjureGetFreshTimestampsResponseV2 getFreshTimestampsV2(
            AuthHeader authHeader, String namespace, ConjureGetFreshTimestampsRequestV2 request) {
        return shortTimeoutProxy.getFreshTimestampsV2(authHeader, namespace, request);
    }

    @Override
    public ConjureSingleTimestamp getFreshTimestamp(AuthHeader authHeader, String namespace) {
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
    public GetCommitTimestampResponse getCommitTimestamp(
            AuthHeader authHeader, String namespace, GetCommitTimestampRequest request) {
        return shortTimeoutProxy.getCommitTimestamp(authHeader, namespace, request);
    }

    @Override
    public AcquireNamedMinimumTimestampLeaseResponse acquireNamedMinimumTimestampLease(
            AuthHeader authHeader,
            Namespace namespace,
            NamedMinimumTimestampLessor lessor,
            AcquireNamedMinimumTimestampLeaseRequest request) {
        return shortTimeoutProxy.acquireNamedMinimumTimestampLease(authHeader, namespace, lessor, request);
    }

    @Override
    public long getSmallestLeasedNamedTimestamp(
            AuthHeader authHeader, Namespace namespace, NamedMinimumTimestampLessor lessor) {
        return shortTimeoutProxy.getSmallestLeasedNamedTimestamp(authHeader, namespace, lessor);
    }
}
