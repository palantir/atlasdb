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

package com.palantir.lock.client;

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

public class NamespacedConjureTimelockServiceImpl implements NamespacedConjureTimelockService {
    private static final AuthHeader AUTH_HEADER = AuthHeader.valueOf("Bearer omitted");
    private final String namespace;
    private final ConjureTimelockService conjureTimelockService;

    public NamespacedConjureTimelockServiceImpl(ConjureTimelockService conjureTimelockService, String namespace) {
        this.namespace = namespace;
        this.conjureTimelockService = conjureTimelockService;
    }

    @Override
    public ConjureStartTransactionsResponse startTransactions(ConjureStartTransactionsRequest request) {
        return conjureTimelockService.startTransactions(AUTH_HEADER, namespace, request);
    }

    @Override
    public ConjureGetFreshTimestampsResponse getFreshTimestamps(ConjureGetFreshTimestampsRequest request) {
        return conjureTimelockService.getFreshTimestamps(AUTH_HEADER, namespace, request);
    }

    @Override
    public GetCommitTimestampsResponse getCommitTimestamps(GetCommitTimestampsRequest request) {
        return conjureTimelockService.getCommitTimestamps(AUTH_HEADER, namespace, request);
    }

    @Override
    public LeaderTime leaderTime() {
        return conjureTimelockService.leaderTime(AUTH_HEADER, namespace);
    }

    @Override
    public ConjureLockResponse lock(ConjureLockRequest request) {
        return conjureTimelockService.lock(AUTH_HEADER, namespace, request);
    }

    @Override
    public ConjureWaitForLocksResponse waitForLocks(ConjureLockRequest request) {
        return conjureTimelockService.waitForLocks(AUTH_HEADER, namespace, request);
    }

    @Override
    public ConjureRefreshLocksResponse refreshLocks(ConjureRefreshLocksRequest request) {
        return conjureTimelockService.refreshLocks(AUTH_HEADER, namespace, request);
    }

    @Override
    public ConjureUnlockResponse unlock(ConjureUnlockRequest request) {
        return conjureTimelockService.unlock(AUTH_HEADER, namespace, request);
    }
}
