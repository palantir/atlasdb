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

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
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
import com.palantir.atlasdb.timelock.api.ConjureTimelockServiceBlocking;
import com.palantir.atlasdb.timelock.api.ConjureUnlockRequest;
import com.palantir.atlasdb.timelock.api.ConjureUnlockRequestV2;
import com.palantir.atlasdb.timelock.api.ConjureUnlockResponse;
import com.palantir.atlasdb.timelock.api.ConjureUnlockResponseV2;
import com.palantir.atlasdb.timelock.api.ConjureWaitForLocksResponse;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsRequest;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsResponse;
import com.palantir.atlasdb.timelock.api.GetOneCommitTimestampRequest;
import com.palantir.atlasdb.timelock.api.GetOneCommitTimestampResponse;
import com.palantir.dialogue.BinaryRequestBody;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.tokens.auth.AuthHeader;
import java.io.InputStream;
import java.util.function.Supplier;
import javax.ws.rs.core.StreamingOutput;

public class DialogueAdaptingConjureTimelockService implements ConjureTimelockService {
    private final ConjureTimelockServiceBlocking dialogueDelegate;
    private final ConjureTimelockServiceBlockingMetrics conjureTimelockServiceBlockingMetrics;

    public DialogueAdaptingConjureTimelockService(
            ConjureTimelockServiceBlocking dialogueDelegate,
            ConjureTimelockServiceBlockingMetrics conjureTimelockServiceBlockingMetrics) {
        this.dialogueDelegate = dialogueDelegate;
        this.conjureTimelockServiceBlockingMetrics = conjureTimelockServiceBlockingMetrics;
    }

    @Override
    public ConjureStartTransactionsResponse startTransactions(
            AuthHeader authHeader, String namespace, ConjureStartTransactionsRequest request) {
        return executeInstrumented(
                () -> dialogueDelegate.startTransactions(authHeader, namespace, request),
                () -> conjureTimelockServiceBlockingMetrics.startTransactions().time(),
                conjureTimelockServiceBlockingMetrics::startTransactionErrors);
    }

    @Override
    public ConjureGetFreshTimestampsResponse getFreshTimestamps(
            AuthHeader authHeader, String namespace, ConjureGetFreshTimestampsRequest request) {
        return dialogueDelegate.getFreshTimestamps(authHeader, namespace, request);
    }

    @Override
    public ConjureStartOneTransactionResponse startOneTransaction(
            AuthHeader authHeader, String namespace, ConjureStartOneTransactionRequest request) {
        return dialogueDelegate.startOneTransaction(authHeader, namespace, request);
    }

    @Override
    public ConjureGetFreshTimestampResponse getFreshTimestamp(AuthHeader authHeader, String namespace) {
        return dialogueDelegate.getFreshTimestamp(authHeader, namespace);
    }

    @Override
    public LeaderTime leaderTime(AuthHeader authHeader, String namespace) {
        return executeInstrumented(
                () -> dialogueDelegate.leaderTime(authHeader, namespace),
                () -> conjureTimelockServiceBlockingMetrics.leaderTime().time(),
                conjureTimelockServiceBlockingMetrics::leaderTimeErrors);
    }

    @Override
    public ConjureLockResponse lock(AuthHeader authHeader, String namespace, ConjureLockRequest request) {
        return dialogueDelegate.lock(authHeader, namespace, request);
    }

    @Override
    public ConjureLockResponseV2 lockV2(AuthHeader authHeader, String namespace, ConjureLockRequest request) {
        return dialogueDelegate.lockV2(authHeader, namespace, request);
    }

    @Override
    public ConjureWaitForLocksResponse waitForLocks(
            AuthHeader authHeader, String namespace, ConjureLockRequest request) {
        return dialogueDelegate.waitForLocks(authHeader, namespace, request);
    }

    @Override
    public ConjureRefreshLocksResponse refreshLocks(
            AuthHeader authHeader, String namespace, ConjureRefreshLocksRequest request) {
        return dialogueDelegate.refreshLocks(authHeader, namespace, request);
    }

    @Override
    public ConjureRefreshLocksResponseV2 refreshLocksV2(
            AuthHeader authHeader, String namespace, ConjureRefreshLocksRequestV2 request) {
        return dialogueDelegate.refreshLocksV2(authHeader, namespace, request);
    }

    @Override
    public ConjureUnlockResponse unlock(AuthHeader authHeader, String namespace, ConjureUnlockRequest request) {
        return dialogueDelegate.unlock(authHeader, namespace, request);
    }

    @Override
    public ConjureUnlockResponseV2 unlockV2(AuthHeader authHeader, String namespace, ConjureUnlockRequestV2 request) {
        return dialogueDelegate.unlockV2(authHeader, namespace, request);
    }

    @Override
    public GetCommitTimestampsResponse getCommitTimestamps(
            AuthHeader authHeader, String namespace, GetCommitTimestampsRequest request) {
        return dialogueDelegate.getCommitTimestamps(authHeader, namespace, request);
    }

    @Override
    public GetOneCommitTimestampResponse getOneCommitTimestamp(
            AuthHeader authHeader, String namespace, GetOneCommitTimestampRequest request) {
        return dialogueDelegate.getOneCommitTimestamp(authHeader, namespace, request);
    }

    @Override
    @SuppressWarnings("MustBeClosedChecker") // The responsibility here is not mine.
    public StreamingOutput runCommands(AuthHeader authHeader, String namespace, InputStream requests) {
        return dialogueDelegate.runCommands(authHeader, namespace, BinaryRequestBody.of(requests))::transferTo;
    }

    private <T> T executeInstrumented(
            Supplier<T> supplier, Supplier<Timer.Context> timerSupplier, Supplier<Meter> meterSupplier) {
        try (Timer.Context timer = timerSupplier.get()) {
            return supplier.get();
        } catch (RuntimeException e) {
            meterSupplier.get().mark();
            throw e;
        }
    }
}
