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

package com.palantir.atlasdb.timelock;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.http.RedirectRetryTargeter;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsRequest;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsResponse;
import com.palantir.atlasdb.timelock.api.ConjureIdentifiedVersion;
import com.palantir.atlasdb.timelock.api.ConjureLockDescriptor;
import com.palantir.atlasdb.timelock.api.ConjureLockRequest;
import com.palantir.atlasdb.timelock.api.ConjureLockResponse;
import com.palantir.atlasdb.timelock.api.ConjureLockToken;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksRequest;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksResponse;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsRequest;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.atlasdb.timelock.api.ConjureTimelockService;
import com.palantir.atlasdb.timelock.api.ConjureTimelockServiceEndpoints;
import com.palantir.atlasdb.timelock.api.ConjureUnlockRequest;
import com.palantir.atlasdb.timelock.api.ConjureUnlockResponse;
import com.palantir.atlasdb.timelock.api.ConjureWaitForLocksResponse;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsRequest;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsResponse;
import com.palantir.atlasdb.timelock.api.SuccessfulLockResponse;
import com.palantir.atlasdb.timelock.api.UndertowConjureTimelockService;
import com.palantir.atlasdb.timelock.api.UnsuccessfulLockResponse;
import com.palantir.conjure.java.undertow.lib.UndertowService;
import com.palantir.lock.ByteArrayLockDescriptor;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.client.IdentifiedLockRequest;
import com.palantir.lock.client.ImmutableIdentifiedLockRequest;
import com.palantir.lock.v2.ImmutableWaitForLocksRequest;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.LockResponseV2;
import com.palantir.lock.v2.LockResponseV2.Visitor;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.timestamp.TimestampRange;
import com.palantir.tokens.auth.AuthHeader;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

public final class ConjureTimelockResource implements UndertowConjureTimelockService {
    private final ConjureResourceExceptionHandler exceptionHandler;
    private final Function<String, AsyncTimelockService> timelockServices;

    @VisibleForTesting
    ConjureTimelockResource(
            RedirectRetryTargeter redirectRetryTargeter, Function<String, AsyncTimelockService> timelockServices) {
        this.exceptionHandler = new ConjureResourceExceptionHandler(redirectRetryTargeter);
        this.timelockServices = timelockServices;
    }

    public static UndertowService undertow(
            RedirectRetryTargeter redirectRetryTargeter, Function<String, AsyncTimelockService> timelockServices) {
        return ConjureTimelockServiceEndpoints.of(new ConjureTimelockResource(redirectRetryTargeter, timelockServices));
    }

    public static ConjureTimelockService jersey(
            RedirectRetryTargeter redirectRetryTargeter, Function<String, AsyncTimelockService> timelockServices) {
        return new JerseyAdapter(new ConjureTimelockResource(redirectRetryTargeter, timelockServices));
    }

    @Override
    public ListenableFuture<ConjureStartTransactionsResponse> startTransactions(
            AuthHeader _authHeader, String namespace, ConjureStartTransactionsRequest request) {
        return handleExceptions(() -> forNamespace(namespace).startTransactionsWithWatches(request));
    }

    @Override
    public ListenableFuture<ConjureGetFreshTimestampsResponse> getFreshTimestamps(
            AuthHeader _authHeader, String namespace, ConjureGetFreshTimestampsRequest request) {
        return handleExceptions(() -> {
            ListenableFuture<TimestampRange> rangeFuture =
                    forNamespace(namespace).getFreshTimestampsAsync(request.getNumTimestamps());
            return Futures.transform(
                    rangeFuture,
                    range -> ConjureGetFreshTimestampsResponse.of(range.getLowerBound(), range.getUpperBound()),
                    MoreExecutors.directExecutor());
        });
    }

    @Override
    public ListenableFuture<LeaderTime> leaderTime(AuthHeader _authHeader, String namespace) {
        return handleExceptions(() -> forNamespace(namespace).leaderTime());
    }

    @Override
    public ListenableFuture<ConjureLockResponse> lock(
            AuthHeader _authHeader, String namespace, ConjureLockRequest request) {
        return handleExceptions(() -> {
            IdentifiedLockRequest lockRequest = ImmutableIdentifiedLockRequest.builder()
                    .lockDescriptors(fromConjureLockDescriptors(request.getLockDescriptors()))
                    .clientDescription(request.getClientDescription())
                    .requestId(request.getRequestId())
                    .acquireTimeoutMs(request.getAcquireTimeoutMs())
                    .build();
            ListenableFuture<LockResponseV2> tokenFuture =
                    forNamespace(namespace).lock(lockRequest);
            return Futures.transform(
                    tokenFuture,
                    token -> token.accept(Visitor.of(
                            success -> ConjureLockResponse.successful(SuccessfulLockResponse.of(
                                    ConjureLockToken.of(success.getToken().getRequestId()), success.getLease())),
                            _failure -> ConjureLockResponse.unsuccessful(UnsuccessfulLockResponse.of()))),
                    MoreExecutors.directExecutor());
        });
    }

    @Override
    public ListenableFuture<ConjureWaitForLocksResponse> waitForLocks(
            AuthHeader _authHeader, String namespace, ConjureLockRequest request) {
        return handleExceptions(() -> {
            WaitForLocksRequest lockRequest = ImmutableWaitForLocksRequest.builder()
                    .lockDescriptors(fromConjureLockDescriptors(request.getLockDescriptors()))
                    .clientDescription(request.getClientDescription())
                    .requestId(request.getRequestId())
                    .acquireTimeoutMs(request.getAcquireTimeoutMs())
                    .build();
            ListenableFuture<WaitForLocksResponse> tokenFuture =
                    forNamespace(namespace).waitForLocks(lockRequest);
            return Futures.transform(
                    tokenFuture,
                    token -> ConjureWaitForLocksResponse.of(token.wasSuccessful()),
                    MoreExecutors.directExecutor());
        });
    }

    private static Set<LockDescriptor> fromConjureLockDescriptors(Set<ConjureLockDescriptor> lockDescriptors) {
        Set<LockDescriptor> descriptors = new HashSet<>(lockDescriptors.size());
        for (ConjureLockDescriptor descriptor : lockDescriptors) {
            descriptors.add(ByteArrayLockDescriptor.of(descriptor.get().asNewByteArray()));
        }
        return descriptors;
    }

    @Override
    public ListenableFuture<ConjureRefreshLocksResponse> refreshLocks(
            AuthHeader _authHeader, String namespace, ConjureRefreshLocksRequest request) {
        return handleExceptions(() -> Futures.transform(
                forNamespace(namespace).refreshLockLeases(fromConjureLockTokens(request.getTokens())),
                refreshed -> ConjureRefreshLocksResponse.of(
                        toConjureLockTokens(refreshed.refreshedTokens()), refreshed.getLease()),
                MoreExecutors.directExecutor()));
    }

    @Override
    public ListenableFuture<ConjureUnlockResponse> unlock(
            AuthHeader _authHeader, String namespace, ConjureUnlockRequest request) {
        return handleExceptions(() -> Futures.transform(
                forNamespace(namespace).unlock(fromConjureLockTokens(request.getTokens())),
                unlocked -> ConjureUnlockResponse.of(toConjureLockTokens(unlocked)),
                MoreExecutors.directExecutor()));
    }

    private static Set<LockToken> fromConjureLockTokens(Set<ConjureLockToken> lockTokens) {
        Set<LockToken> tokens = new HashSet<>(lockTokens.size());
        for (ConjureLockToken token : lockTokens) {
            tokens.add(LockToken.of(token.getRequestId()));
        }
        return tokens;
    }

    private static Set<ConjureLockToken> toConjureLockTokens(Set<LockToken> lockTokens) {
        Set<ConjureLockToken> tokens = new HashSet<>(lockTokens.size());
        for (LockToken token : lockTokens) {
            tokens.add(ConjureLockToken.of(token.getRequestId()));
        }
        return tokens;
    }

    @Override
    public ListenableFuture<GetCommitTimestampsResponse> getCommitTimestamps(
            AuthHeader _authHeader, String namespace, GetCommitTimestampsRequest request) {
        return handleExceptions(() -> forNamespace(namespace)
                .getCommitTimestamps(
                        request.getNumTimestamps(),
                        request.getLastKnownVersion().map(this::toIdentifiedVersion)));
    }

    private AsyncTimelockService forNamespace(String namespace) {
        return timelockServices.apply(namespace);
    }

    private <T> ListenableFuture<T> handleExceptions(Supplier<ListenableFuture<T>> supplier) {
        return exceptionHandler.handleExceptions(supplier);
    }

    public static final class JerseyAdapter implements ConjureTimelockService {
        private final ConjureTimelockResource resource;

        private JerseyAdapter(ConjureTimelockResource resource) {
            this.resource = resource;
        }

        @Override
        public ConjureStartTransactionsResponse startTransactions(
                AuthHeader authHeader, String namespace, ConjureStartTransactionsRequest request) {
            return unwrap(resource.startTransactions(authHeader, namespace, request));
        }

        @Override
        public ConjureGetFreshTimestampsResponse getFreshTimestamps(
                AuthHeader authHeader, String namespace, ConjureGetFreshTimestampsRequest request) {
            return unwrap(resource.getFreshTimestamps(authHeader, namespace, request));
        }

        @Override
        public LeaderTime leaderTime(AuthHeader authHeader, String namespace) {
            return unwrap(resource.leaderTime(authHeader, namespace));
        }

        @Override
        public ConjureLockResponse lock(AuthHeader authHeader, String namespace, ConjureLockRequest request) {
            return unwrap(resource.lock(authHeader, namespace, request));
        }

        @Override
        public ConjureWaitForLocksResponse waitForLocks(
                AuthHeader authHeader, String namespace, ConjureLockRequest request) {
            return unwrap(resource.waitForLocks(authHeader, namespace, request));
        }

        @Override
        public ConjureRefreshLocksResponse refreshLocks(
                AuthHeader authHeader, String namespace, ConjureRefreshLocksRequest request) {
            return unwrap(resource.refreshLocks(authHeader, namespace, request));
        }

        @Override
        public ConjureUnlockResponse unlock(AuthHeader authHeader, String namespace, ConjureUnlockRequest request) {
            return unwrap(resource.unlock(authHeader, namespace, request));
        }

        @Override
        public GetCommitTimestampsResponse getCommitTimestamps(
                AuthHeader authHeader, String namespace, GetCommitTimestampsRequest request) {
            return unwrap(resource.getCommitTimestamps(authHeader, namespace, request));
        }

        private static <T> T unwrap(ListenableFuture<T> future) {
            return AtlasFutures.getUnchecked(future);
        }
    }

    private LockWatchVersion toIdentifiedVersion(ConjureIdentifiedVersion conjureIdentifiedVersion) {
        return LockWatchVersion.of(conjureIdentifiedVersion.getId(), conjureIdentifiedVersion.getVersion());
    }
}
