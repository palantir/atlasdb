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
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsRequest;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsRequestV2;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsResponse;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsResponseV2;
import com.palantir.atlasdb.timelock.api.ConjureLockRequest;
import com.palantir.atlasdb.timelock.api.ConjureLockResponse;
import com.palantir.atlasdb.timelock.api.ConjureLockToken;
import com.palantir.atlasdb.timelock.api.ConjureLockTokenV2;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksRequest;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksRequestV2;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksResponse;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksResponseV2;
import com.palantir.atlasdb.timelock.api.ConjureSingleTimestamp;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsRequest;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.atlasdb.timelock.api.ConjureTimelockService;
import com.palantir.atlasdb.timelock.api.ConjureTimelockServiceBlocking;
import com.palantir.atlasdb.timelock.api.ConjureTimestampRange;
import com.palantir.atlasdb.timelock.api.ConjureUnlockRequest;
import com.palantir.atlasdb.timelock.api.ConjureUnlockRequestV2;
import com.palantir.atlasdb.timelock.api.ConjureUnlockResponse;
import com.palantir.atlasdb.timelock.api.ConjureUnlockResponseV2;
import com.palantir.atlasdb.timelock.api.ConjureWaitForLocksResponse;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampRequest;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampResponse;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsRequest;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsResponse;
import com.palantir.conjure.java.api.errors.UnknownRemoteException;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.tokens.auth.AuthHeader;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class DialogueAdaptingConjureTimelockService implements ConjureTimelockService {
    private static final int NOT_FOUND = 404;

    private final ConjureTimelockServiceBlocking dialogueDelegate;
    private final ConjureTimelockServiceBlockingMetrics conjureTimelockServiceBlockingMetrics;

    private final ServerApiVersionGuesser serverApiVersionGuesser;

    public DialogueAdaptingConjureTimelockService(
            ConjureTimelockServiceBlocking dialogueDelegate,
            ConjureTimelockServiceBlockingMetrics conjureTimelockServiceBlockingMetrics) {
        this.dialogueDelegate = dialogueDelegate;
        this.conjureTimelockServiceBlockingMetrics = conjureTimelockServiceBlockingMetrics;
        this.serverApiVersionGuesser = new ServerApiVersionGuesser();
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
    public ConjureGetFreshTimestampsResponseV2 getFreshTimestampsV2(
            AuthHeader authHeader, String namespace, ConjureGetFreshTimestampsRequestV2 request) {
        return serverApiVersionGuesser.runUnderSuspicion(
                () -> dialogueDelegate.getFreshTimestampsV2(authHeader, namespace, request), () -> {
                    ConjureGetFreshTimestampsResponse response = dialogueDelegate.getFreshTimestamps(
                            authHeader, namespace, ConjureGetFreshTimestampsRequest.of(request.get()));
                    return ConjureGetFreshTimestampsResponseV2.of(ConjureTimestampRange.of(
                            response.getInclusiveLower(),
                            response.getInclusiveUpper() - response.getInclusiveLower() + 1));
                });
    }

    @Override
    public ConjureSingleTimestamp getFreshTimestamp(AuthHeader authHeader, String namespace) {
        return serverApiVersionGuesser.runUnderSuspicion(
                () -> dialogueDelegate.getFreshTimestamp(authHeader, namespace), () -> {
                    ConjureGetFreshTimestampsResponse response = dialogueDelegate.getFreshTimestamps(
                            authHeader, namespace, ConjureGetFreshTimestampsRequest.of(1));
                    return ConjureSingleTimestamp.of(response.getInclusiveLower());
                });
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
        return serverApiVersionGuesser.runUnderSuspicion(
                () -> dialogueDelegate.refreshLocksV2(authHeader, namespace, request), () -> {
                    ConjureRefreshLocksRequest v1Request = ConjureRefreshLocksRequest.of(fromV2Tokens(request.get()));
                    ConjureRefreshLocksResponse lockResponse =
                            dialogueDelegate.refreshLocks(authHeader, namespace, v1Request);
                    return ConjureRefreshLocksResponseV2.of(
                            toV2Tokens(lockResponse.getRefreshedTokens()), lockResponse.getLease());
                });
    }

    @Override
    public ConjureUnlockResponse unlock(AuthHeader authHeader, String namespace, ConjureUnlockRequest request) {
        return dialogueDelegate.unlock(authHeader, namespace, request);
    }

    @Override
    public ConjureUnlockResponseV2 unlockV2(AuthHeader authHeader, String namespace, ConjureUnlockRequestV2 request) {
        return serverApiVersionGuesser.runUnderSuspicion(
                () -> dialogueDelegate.unlockV2(authHeader, namespace, request), () -> {
                    ConjureUnlockRequest v1Request = ConjureUnlockRequest.of(fromV2Tokens(request.get()));
                    return ConjureUnlockResponseV2.of(toV2Tokens(dialogueDelegate
                            .unlock(authHeader, namespace, v1Request)
                            .getTokens()));
                });
    }

    @Override
    public GetCommitTimestampsResponse getCommitTimestamps(
            AuthHeader authHeader, String namespace, GetCommitTimestampsRequest request) {
        return dialogueDelegate.getCommitTimestamps(authHeader, namespace, request);
    }

    @Override
    public GetCommitTimestampResponse getCommitTimestamp(
            AuthHeader authHeader, String namespace, GetCommitTimestampRequest request) {
        return dialogueDelegate.getCommitTimestamp(authHeader, namespace, request);
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

    private Set<ConjureLockTokenV2> toV2Tokens(Set<ConjureLockToken> v1Tokens) {
        return v1Tokens.stream()
                .map(token -> ConjureLockTokenV2.of(token.getRequestId()))
                .collect(Collectors.toSet());
    }

    private Set<ConjureLockToken> fromV2Tokens(Set<ConjureLockTokenV2> v2Tokens) {
        return v2Tokens.stream().map(token -> ConjureLockToken.of(token.get())).collect(Collectors.toSet());
    }

    private static class ServerApiVersionGuesser {
        private final AtomicBoolean suspectOldVersion;

        private ServerApiVersionGuesser() {
            this.suspectOldVersion = new AtomicBoolean();
        }

        private boolean shouldUseNewEndpoint() {
            if (suspectOldVersion.get()) {
                return ThreadLocalRandom.current().nextDouble() < 0.01;
            }
            return true;
        }

        public <T> T runUnderSuspicion(Supplier<T> newFunction, Supplier<T> legacyFunction) {
            if (shouldUseNewEndpoint()) {
                return runNewFunctionFirst(newFunction, legacyFunction);
            }
            return legacyFunction.get();
        }

        private <T> T runNewFunctionFirst(Supplier<T> newFunction, Supplier<T> legacyFunction) {
            try {
                T candidateOutput = newFunction.get();
                suspectOldVersion.set(false);
                return candidateOutput;
            } catch (UnknownRemoteException exception) {
                if (exception.getStatus() != NOT_FOUND) {
                    throw exception;
                }
                suspectOldVersion.set(true);
                return legacyFunction.get();
            }
        }
    }
}
