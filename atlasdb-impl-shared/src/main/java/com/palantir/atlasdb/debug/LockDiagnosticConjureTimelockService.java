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
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsRequest;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.atlasdb.timelock.api.ConjureTimelockService;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsRequest;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsResponse;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.tokens.auth.AuthHeader;

/**
 * TODO(fdesouza): Remove this once PDS-95791 is resolved.
 * @deprecated Remove this once PDS-95791 is resolved.
 */
@Deprecated
public class LockDiagnosticConjureTimelockService implements ConjureTimelockService {
    private final ConjureTimelockService conjureDelegate;
    private final ClientLockDiagnosticCollector lockDiagnosticCollector;

    public LockDiagnosticConjureTimelockService(
            ConjureTimelockService conjureDelegate,
            ClientLockDiagnosticCollector lockDiagnosticCollector) {
        this.conjureDelegate = conjureDelegate;
        this.lockDiagnosticCollector = lockDiagnosticCollector;
    }

    @Override
    public ConjureStartTransactionsResponse startTransactions(AuthHeader authHeader, String namespace,
            ConjureStartTransactionsRequest request) {
        ConjureStartTransactionsResponse response = conjureDelegate.startTransactions(authHeader, namespace, request);
        lockDiagnosticCollector.collect(
                response.getTimestamps().stream(),
                response.getImmutableTimestamp().getImmutableTimestamp(),
                request.getRequestId());
        return response;
    }

    @Override
    public ConjureGetFreshTimestampsResponse getFreshTimestamps(AuthHeader authHeader, String namespace,
            ConjureGetFreshTimestampsRequest request) {
        return conjureDelegate.getFreshTimestamps(authHeader, namespace, request);
    }

    @Override
    public LeaderTime leaderTime(AuthHeader authHeader, String namespace) {
        return conjureDelegate.leaderTime(authHeader, namespace);
    }

    @Override
    public GetCommitTimestampsResponse getCommitTimestamps(AuthHeader authHeader, String namespace,
            GetCommitTimestampsRequest request) {
        return conjureDelegate.getCommitTimestamps(authHeader, namespace, request);
    }
}
