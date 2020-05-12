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
import com.palantir.atlasdb.timelock.api.ConjureTimelockServiceBlocking;
import com.palantir.atlasdb.timelock.api.ConjureUnlockRequest;
import com.palantir.atlasdb.timelock.api.ConjureUnlockResponse;
import com.palantir.atlasdb.timelock.api.ConjureWaitForLocksResponse;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsRequest;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsResponse;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.tokens.auth.AuthHeader;

public class DialogueAdaptingConjureTimelockService implements ConjureTimelockService {
    private final ConjureTimelockServiceBlocking dialogueDelegate;

    public DialogueAdaptingConjureTimelockService(ConjureTimelockServiceBlocking dialogueDelegate) {
        this.dialogueDelegate = dialogueDelegate;
    }

    @Override
    public ConjureStartTransactionsResponse startTransactions(AuthHeader authHeader, String namespace,
            ConjureStartTransactionsRequest request) {
        return dialogueDelegate.startTransactions(authHeader, namespace, request);
    }

    @Override
    public ConjureGetFreshTimestampsResponse getFreshTimestamps(AuthHeader authHeader, String namespace,
            ConjureGetFreshTimestampsRequest request) {
        return dialogueDelegate.getFreshTimestamps(authHeader, namespace, request);
    }

    @Override
    public LeaderTime leaderTime(AuthHeader authHeader, String namespace) {
        return dialogueDelegate.leaderTime(authHeader, namespace);
    }

    @Override
    public ConjureLockResponse lock(AuthHeader authHeader, String namespace, ConjureLockRequest request) {
        return dialogueDelegate.lock(authHeader, namespace, request);
    }

    @Override
    public ConjureWaitForLocksResponse waitForLocks(AuthHeader authHeader, String namespace,
            ConjureLockRequest request) {
        return dialogueDelegate.waitForLocks(authHeader, namespace, request);
    }

    @Override
    public ConjureRefreshLocksResponse refreshLocks(AuthHeader authHeader, String namespace,
            ConjureRefreshLocksRequest request) {
        return dialogueDelegate.refreshLocks(authHeader, namespace, request);
    }

    @Override
    public ConjureUnlockResponse unlock(AuthHeader authHeader, String namespace, ConjureUnlockRequest request) {
        return dialogueDelegate.unlock(authHeader, namespace, request);
    }

    @Override
    public GetCommitTimestampsResponse getCommitTimestamps(AuthHeader authHeader, String namespace,
            GetCommitTimestampsRequest request) {
        return dialogueDelegate.getCommitTimestamps(authHeader, namespace, request);
    }
}
