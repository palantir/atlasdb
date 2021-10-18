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

package com.palantir.atlasdb.debug;

import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsRequest;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsResponse;
import com.palantir.atlasdb.timelock.api.ConjureLockImmutableTimestampResponse;
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
import java.util.Optional;

/**
 * TODO(fdesouza): Remove this once PDS-95791 is resolved.
 * @deprecated Remove this once PDS-95791 is resolved.
 */
@Deprecated
public class LockDiagnosticConjureTimelockService implements ConjureTimelockService {
    private final ConjureTimelockService conjureDelegate;
    private final ClientLockDiagnosticCollector lockDiagnosticCollector;
    private final LocalLockTracker localLockTracker;

    public LockDiagnosticConjureTimelockService(
            ConjureTimelockService conjureDelegate,
            ClientLockDiagnosticCollector lockDiagnosticCollector,
            LocalLockTracker localLockTracker) {
        this.conjureDelegate = conjureDelegate;
        this.lockDiagnosticCollector = lockDiagnosticCollector;
        this.localLockTracker = localLockTracker;
    }

    @Override
    public ConjureStartTransactionsResponse startTransactions(
            AuthHeader authHeader, String namespace, ConjureStartTransactionsRequest request) {
        ConjureStartTransactionsResponse response = conjureDelegate.startTransactions(authHeader, namespace, request);
        lockDiagnosticCollector.collect(
                response.getTimestamps().stream(),
                response.getImmutableTimestamp().getImmutableTimestamp(),
                request.getRequestId());
        return response;
    }

    @Override
    public ConjureGetFreshTimestampsResponse getFreshTimestamps(
            AuthHeader authHeader, String namespace, ConjureGetFreshTimestampsRequest request) {
        return conjureDelegate.getFreshTimestamps(authHeader, namespace, request);
    }

    @Override
    public LeaderTime leaderTime(AuthHeader authHeader, String namespace) {
        return conjureDelegate.leaderTime(authHeader, namespace);
    }

    @Override
    public ConjureLockResponse lock(AuthHeader authHeader, String namespace, ConjureLockRequest request) {
        request.getClientDescription()
                .flatMap(LockDiagnosticConjureTimelockService::tryParseStartTimestamp)
                .ifPresent(startTimestamp -> lockDiagnosticCollector.collect(
                        startTimestamp, request.getRequestId(), request.getLockDescriptors()));
        ConjureLockResponse response = conjureDelegate.lock(authHeader, namespace, request);
        localLockTracker.logLockResponse(request.getLockDescriptors(), response);
        return response;
    }

    @Override
    public ConjureLockImmutableTimestampResponse lockImmutableTimestamp(AuthHeader authHeader, String namespace) {
        // TODO(gs): localLockTracker?
        ConjureLockImmutableTimestampResponse response = conjureDelegate.lockImmutableTimestamp(authHeader, namespace);
        localLockTracker.logLockImmutableTimestampResponse(response);
        return response;
    }

    @Override
    public ConjureWaitForLocksResponse waitForLocks(
            AuthHeader authHeader, String namespace, ConjureLockRequest request) {
        request.getClientDescription()
                .flatMap(LockDiagnosticConjureTimelockService::tryParseStartTimestamp)
                .ifPresent(startTimestamp -> lockDiagnosticCollector.collect(
                        startTimestamp, request.getRequestId(), request.getLockDescriptors()));
        ConjureWaitForLocksResponse response = conjureDelegate.waitForLocks(authHeader, namespace, request);
        localLockTracker.logWaitForLocksResponse(request.getLockDescriptors(), response);
        return response;
    }

    @Override
    public ConjureRefreshLocksResponse refreshLocks(
            AuthHeader authHeader, String namespace, ConjureRefreshLocksRequest request) {
        ConjureRefreshLocksResponse response = conjureDelegate.refreshLocks(authHeader, namespace, request);
        localLockTracker.logRefreshResponse(request.getTokens(), response);
        return response;
    }

    @Override
    public ConjureUnlockResponse unlock(AuthHeader authHeader, String namespace, ConjureUnlockRequest request) {
        ConjureUnlockResponse response = conjureDelegate.unlock(authHeader, namespace, request);
        localLockTracker.logUnlockResponse(request.getTokens(), response);
        return response;
    }

    @Override
    public GetCommitTimestampsResponse getCommitTimestamps(
            AuthHeader authHeader, String namespace, GetCommitTimestampsRequest request) {
        return conjureDelegate.getCommitTimestamps(authHeader, namespace, request);
    }

    private static Optional<Long> tryParseStartTimestamp(String description) {
        try {
            return Optional.of(Long.parseLong(description));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }
}
