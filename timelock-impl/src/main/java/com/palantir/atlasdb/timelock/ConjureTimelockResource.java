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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.http.RedirectRetryTargeter;
import com.palantir.atlasdb.timelock.api.AcquireNamedTimestampMinimumLeaseRequest;
import com.palantir.atlasdb.timelock.api.AcquireNamedTimestampMinimumLeaseResponse;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsRequest;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsRequestV2;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsResponse;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsResponseV2;
import com.palantir.atlasdb.timelock.api.ConjureIdentifiedVersion;
import com.palantir.atlasdb.timelock.api.ConjureLockDescriptor;
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
import com.palantir.atlasdb.timelock.api.ConjureTimelockServiceEndpoints;
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
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.SuccessfulLockResponse;
import com.palantir.atlasdb.timelock.api.UndertowConjureTimelockService;
import com.palantir.atlasdb.timelock.api.UnsuccessfulLockResponse;
import com.palantir.conjure.java.undertow.lib.RequestContext;
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
import com.palantir.lock.v2.RefreshLockResponseV2;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.lock.watch.ConjureLockRequestMetadataUtils;
import com.palantir.lock.watch.ConjureLockRequestMetadataUtils.ConjureMetadataConversionResult;
import com.palantir.lock.watch.LockRequestMetadata;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.timestamp.TimestampRange;
import com.palantir.tokens.auth.AuthHeader;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;

public final class ConjureTimelockResource implements UndertowConjureTimelockService {
    private final ConjureResourceExceptionHandler exceptionHandler;
    private final BiFunction<String, Optional<String>, AsyncTimelockService> timelockServices;

    @VisibleForTesting
    ConjureTimelockResource(
            RedirectRetryTargeter redirectRetryTargeter,
            BiFunction<String, Optional<String>, AsyncTimelockService> timelockServices) {
        this.exceptionHandler = new ConjureResourceExceptionHandler(redirectRetryTargeter);
        this.timelockServices = timelockServices;
    }

    public static UndertowService undertow(
            RedirectRetryTargeter redirectRetryTargeter,
            BiFunction<String, Optional<String>, AsyncTimelockService> timelockServices) {
        return ConjureTimelockServiceEndpoints.of(new ConjureTimelockResource(redirectRetryTargeter, timelockServices));
    }

    public static ConjureTimelockService jersey(
            RedirectRetryTargeter redirectRetryTargeter,
            BiFunction<String, Optional<String>, AsyncTimelockService> timelockServices) {
        return new JerseyAdapter(new ConjureTimelockResource(redirectRetryTargeter, timelockServices));
    }

    @Override
    public ListenableFuture<ConjureStartTransactionsResponse> startTransactions(
            AuthHeader authHeader,
            String namespace,
            ConjureStartTransactionsRequest request,
            @Nullable RequestContext context) {
        return handleExceptions(() ->
                forNamespace(namespace, TimelockNamespaces.toUserAgent(context)).startTransactionsWithWatches(request));
    }

    @Override
    public ListenableFuture<AcquireNamedTimestampMinimumLeaseResponse> acquireNamedTimestampMinimumLease(
            AuthHeader authHeader,
            Namespace namespace,
            AcquireNamedTimestampMinimumLeaseRequest request,
            RequestContext requestContext) {
        // TODO(aalouane): Add a method to the AsyncTimeLockService interface
        return null;
    }

    @Override
    public ListenableFuture<ConjureGetFreshTimestampsResponse> getFreshTimestamps(
            AuthHeader authHeader,
            String namespace,
            ConjureGetFreshTimestampsRequest request,
            @Nullable RequestContext context) {
        return getFreshTimestamps(
                namespace,
                TimelockNamespaces.toUserAgent(context),
                request.getNumTimestamps(),
                range -> ConjureGetFreshTimestampsResponse.of(range.getLowerBound(), range.getUpperBound()));
    }

    @Override
    public ListenableFuture<ConjureGetFreshTimestampsResponseV2> getFreshTimestampsV2(
            AuthHeader authHeader,
            String namespace,
            ConjureGetFreshTimestampsRequestV2 request,
            @Nullable RequestContext context) {
        return getFreshTimestamps(
                namespace,
                TimelockNamespaces.toUserAgent(context),
                request.get(),
                range -> ConjureGetFreshTimestampsResponseV2.of(
                        ConjureTimestampRange.of(range.getLowerBound(), range.size())));
    }

    @Override
    public ListenableFuture<ConjureSingleTimestamp> getFreshTimestamp(
            AuthHeader authHeader, String namespace, @Nullable RequestContext context) {
        return getFreshTimestamps(
                namespace,
                TimelockNamespaces.toUserAgent(context),
                1,
                range -> ConjureSingleTimestamp.of(range.getLowerBound()));
    }

    private <T> ListenableFuture<T> getFreshTimestamps(
            String namespace,
            Optional<String> userAgent,
            int timestampsToRetrieve,
            Function<TimestampRange, T> responseWrappingFunction) {
        return handleExceptions(() -> {
            ListenableFuture<TimestampRange> rangeFuture =
                    forNamespace(namespace, userAgent).getFreshTimestampsAsync(timestampsToRetrieve);
            return Futures.transform(rangeFuture, responseWrappingFunction::apply, MoreExecutors.directExecutor());
        });
    }

    @Override
    public ListenableFuture<LeaderTime> leaderTime(
            AuthHeader authHeader, String namespace, @Nullable RequestContext context) {
        return handleExceptions(() ->
                forNamespace(namespace, TimelockNamespaces.toUserAgent(context)).leaderTime());
    }

    @Override
    public ListenableFuture<ConjureLockResponse> lock(
            AuthHeader authHeader, String namespace, ConjureLockRequest request, @Nullable RequestContext context) {
        return handleExceptions(() -> {
            List<LockDescriptor> lockDescriptors = fromConjureLockDescriptors(request.getLockDescriptors());
            Optional<LockRequestMetadata> metadata = request.getMetadata()
                    .map(conjureMetadata -> ConjureLockRequestMetadataUtils.fromConjureIndexEncoded(
                            ConjureMetadataConversionResult.builder()
                                    .conjureMetadata(conjureMetadata)
                                    .lockList(lockDescriptors)
                                    .build()));
            IdentifiedLockRequest lockRequest = ImmutableIdentifiedLockRequest.builder()
                    .lockDescriptors(lockDescriptors)
                    .clientDescription(request.getClientDescription())
                    .requestId(request.getRequestId())
                    .acquireTimeoutMs(request.getAcquireTimeoutMs())
                    .metadata(metadata)
                    .build();
            ListenableFuture<LockResponseV2> tokenFuture = forNamespace(
                            namespace, TimelockNamespaces.toUserAgent(context))
                    .lock(lockRequest);
            return Futures.transform(
                    tokenFuture,
                    token -> token.accept(Visitor.of(
                            success -> ConjureLockResponse.successful(SuccessfulLockResponse.of(
                                    ConjureLockToken.of(success.getToken().getRequestId()), success.getLease())),
                            failure -> ConjureLockResponse.unsuccessful(UnsuccessfulLockResponse.of()))),
                    MoreExecutors.directExecutor());
        });
    }

    @Override
    public ListenableFuture<ConjureWaitForLocksResponse> waitForLocks(
            AuthHeader authHeader, String namespace, ConjureLockRequest request, @Nullable RequestContext context) {
        return handleExceptions(() -> {
            WaitForLocksRequest lockRequest = ImmutableWaitForLocksRequest.builder()
                    .lockDescriptors(fromConjureLockDescriptors(request.getLockDescriptors()))
                    .clientDescription(request.getClientDescription())
                    .requestId(request.getRequestId())
                    .acquireTimeoutMs(request.getAcquireTimeoutMs())
                    .build();
            ListenableFuture<WaitForLocksResponse> tokenFuture = forNamespace(
                            namespace, TimelockNamespaces.toUserAgent(context))
                    .waitForLocks(lockRequest);
            return Futures.transform(
                    tokenFuture,
                    token -> ConjureWaitForLocksResponse.of(token.wasSuccessful()),
                    MoreExecutors.directExecutor());
        });
    }

    private static List<LockDescriptor> fromConjureLockDescriptors(List<ConjureLockDescriptor> lockDescriptors) {
        ImmutableList.Builder<LockDescriptor> descriptors =
                ImmutableList.builderWithExpectedSize(lockDescriptors.size());
        for (ConjureLockDescriptor descriptor : lockDescriptors) {
            descriptors.add(ByteArrayLockDescriptor.of(descriptor.get().asNewByteArray()));
        }
        return descriptors.build();
    }

    @Override
    public ListenableFuture<ConjureRefreshLocksResponse> refreshLocks(
            AuthHeader authHeader,
            String namespace,
            ConjureRefreshLocksRequest request,
            @Nullable RequestContext context) {
        return refreshLocksInternal(
                namespace,
                context,
                fromConjureLockTokens(request.getTokens()),
                refreshed -> ConjureRefreshLocksResponse.of(
                        toConjureLockTokens(refreshed.refreshedTokens()), refreshed.getLease()));
    }

    @Override
    public ListenableFuture<ConjureRefreshLocksResponseV2> refreshLocksV2(
            AuthHeader authHeader,
            String namespace,
            ConjureRefreshLocksRequestV2 request,
            @Nullable RequestContext context) {
        return refreshLocksInternal(
                namespace,
                context,
                fromConjureLockV2Tokens(request.get()),
                refreshed -> ConjureRefreshLocksResponseV2.of(
                        toConjureLockV2Tokens(refreshed.refreshedTokens()), refreshed.getLease()));
    }

    private <T> ListenableFuture<T> refreshLocksInternal(
            String namespace,
            @Nullable RequestContext context,
            Set<LockToken> tokens,
            Function<RefreshLockResponseV2, T> userTokenTranslator) {
        return handleExceptions(() -> Futures.transform(
                forNamespace(namespace, TimelockNamespaces.toUserAgent(context)).refreshLockLeases(tokens),
                userTokenTranslator::apply,
                MoreExecutors.directExecutor()));
    }

    @Override
    public ListenableFuture<ConjureUnlockResponse> unlock(
            AuthHeader authHeader, String namespace, ConjureUnlockRequest request, @Nullable RequestContext context) {
        return unlockInternal(
                namespace,
                context,
                fromConjureLockTokens(request.getTokens()),
                unlocked -> ConjureUnlockResponse.of(toConjureLockTokens(unlocked)));
    }

    @Override
    public ListenableFuture<ConjureUnlockResponseV2> unlockV2(
            AuthHeader authHeader, String namespace, ConjureUnlockRequestV2 request, @Nullable RequestContext context) {
        return unlockInternal(
                namespace,
                context,
                fromConjureLockV2Tokens(request.get()),
                unlocked -> ConjureUnlockResponseV2.of(toConjureLockV2Tokens(unlocked)));
    }

    private <T> ListenableFuture<T> unlockInternal(
            String namespace,
            @Nullable RequestContext context,
            Set<LockToken> tokens,
            Function<Set<LockToken>, T> userTokenTranslator) {
        return handleExceptions(() -> Futures.transform(
                forNamespace(namespace, TimelockNamespaces.toUserAgent(context)).unlock(tokens),
                userTokenTranslator::apply,
                MoreExecutors.directExecutor()));
    }

    // The reason for duplication in the following four methods is for performance reasons; while one might
    // ordinarily use a stream or perhaps some kind of transformation function, this is a hot path.
    private static Set<LockToken> fromConjureLockTokens(Set<ConjureLockToken> lockTokens) {
        Set<LockToken> tokens = Sets.newHashSetWithExpectedSize(lockTokens.size());
        for (ConjureLockToken token : lockTokens) {
            tokens.add(LockToken.of(token.getRequestId()));
        }
        return tokens;
    }

    private static Set<LockToken> fromConjureLockV2Tokens(Set<ConjureLockTokenV2> lockTokens) {
        Set<LockToken> tokens = Sets.newHashSetWithExpectedSize(lockTokens.size());
        for (ConjureLockTokenV2 token : lockTokens) {
            tokens.add(LockToken.of(token.get()));
        }
        return tokens;
    }

    private static Set<ConjureLockToken> toConjureLockTokens(Set<LockToken> lockTokens) {
        Set<ConjureLockToken> tokens = Sets.newHashSetWithExpectedSize(lockTokens.size());
        for (LockToken token : lockTokens) {
            tokens.add(ConjureLockToken.of(token.getRequestId()));
        }
        return tokens;
    }

    private static Set<ConjureLockTokenV2> toConjureLockV2Tokens(Set<LockToken> refreshedTokens) {
        Set<ConjureLockTokenV2> tokens = Sets.newHashSetWithExpectedSize(refreshedTokens.size());
        for (LockToken token : refreshedTokens) {
            tokens.add(ConjureLockTokenV2.of(token.getRequestId()));
        }
        return tokens;
    }

    @Override
    public ListenableFuture<GetCommitTimestampsResponse> getCommitTimestamps(
            AuthHeader authHeader,
            String namespace,
            GetCommitTimestampsRequest request,
            @Nullable RequestContext context) {
        return handleExceptions(() -> getCommitTimestampsInternal(
                namespace, context, request.getNumTimestamps(), request.getLastKnownVersion()));
    }

    @Override
    public ListenableFuture<GetCommitTimestampResponse> getCommitTimestamp(
            AuthHeader authHeader,
            String namespace,
            GetCommitTimestampRequest request,
            @Nullable RequestContext context) {
        return handleExceptions(() -> {
            ListenableFuture<GetCommitTimestampsResponse> responseFuture =
                    getCommitTimestampsInternal(namespace, context, 1, request.getLastKnownVersion());
            return Futures.transform(
                    responseFuture,
                    response ->
                            GetCommitTimestampResponse.of(response.getInclusiveLower(), response.getLockWatchUpdate()),
                    MoreExecutors.directExecutor());
        });
    }

    private ListenableFuture<GetCommitTimestampsResponse> getCommitTimestampsInternal(
            String namespace,
            @Nullable RequestContext context,
            int numTimestamps,
            Optional<ConjureIdentifiedVersion> lockWatchVersion) {
        return forNamespace(namespace, TimelockNamespaces.toUserAgent(context))
                .getCommitTimestamps(numTimestamps, lockWatchVersion.map(this::toIdentifiedVersion));
    }

    private AsyncTimelockService forNamespace(String namespace, Optional<String> userAgent) {
        return timelockServices.apply(namespace, userAgent);
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
            return unwrap(resource.startTransactions(authHeader, namespace, request, null));
        }

        @Override
        public ConjureGetFreshTimestampsResponse getFreshTimestamps(
                AuthHeader authHeader, String namespace, ConjureGetFreshTimestampsRequest request) {
            return unwrap(resource.getFreshTimestamps(authHeader, namespace, request, null));
        }

        @Override
        public ConjureGetFreshTimestampsResponseV2 getFreshTimestampsV2(
                AuthHeader authHeader, String namespace, ConjureGetFreshTimestampsRequestV2 request) {
            return unwrap(resource.getFreshTimestampsV2(authHeader, namespace, request, null));
        }

        @Override
        public ConjureSingleTimestamp getFreshTimestamp(AuthHeader authHeader, String namespace) {
            return unwrap(resource.getFreshTimestamp(authHeader, namespace, null));
        }

        @Override
        public LeaderTime leaderTime(AuthHeader authHeader, String namespace) {
            return unwrap(resource.leaderTime(authHeader, namespace, null));
        }

        @Override
        public ConjureLockResponse lock(AuthHeader authHeader, String namespace, ConjureLockRequest request) {
            return unwrap(resource.lock(authHeader, namespace, request, null));
        }

        @Override
        public ConjureWaitForLocksResponse waitForLocks(
                AuthHeader authHeader, String namespace, ConjureLockRequest request) {
            return unwrap(resource.waitForLocks(authHeader, namespace, request, null));
        }

        @Override
        public ConjureRefreshLocksResponse refreshLocks(
                AuthHeader authHeader, String namespace, ConjureRefreshLocksRequest request) {
            return unwrap(resource.refreshLocks(authHeader, namespace, request, null));
        }

        @Override
        public ConjureRefreshLocksResponseV2 refreshLocksV2(
                AuthHeader authHeader, String namespace, ConjureRefreshLocksRequestV2 request) {
            return unwrap(resource.refreshLocksV2(authHeader, namespace, request, null));
        }

        @Override
        public ConjureUnlockResponse unlock(AuthHeader authHeader, String namespace, ConjureUnlockRequest request) {
            return unwrap(resource.unlock(authHeader, namespace, request, null));
        }

        @Override
        public ConjureUnlockResponseV2 unlockV2(
                AuthHeader authHeader, String namespace, ConjureUnlockRequestV2 request) {
            return unwrap(resource.unlockV2(authHeader, namespace, request, null));
        }

        @Override
        public GetCommitTimestampsResponse getCommitTimestamps(
                AuthHeader authHeader, String namespace, GetCommitTimestampsRequest request) {
            return unwrap(resource.getCommitTimestamps(authHeader, namespace, request, null));
        }

        @Override
        public GetCommitTimestampResponse getCommitTimestamp(
                AuthHeader authHeader, String namespace, GetCommitTimestampRequest request) {
            return unwrap(resource.getCommitTimestamp(authHeader, namespace, request, null));
        }

        @Override
        public AcquireNamedTimestampMinimumLeaseResponse acquireNamedTimestampMinimumLease(
                AuthHeader authHeader, Namespace namespace, AcquireNamedTimestampMinimumLeaseRequest request) {
            return unwrap(resource.acquireNamedTimestampMinimumLease(authHeader, namespace, request, null));
        }

        private static <T> T unwrap(ListenableFuture<T> future) {
            return AtlasFutures.getUnchecked(future);
        }
    }

    private LockWatchVersion toIdentifiedVersion(ConjureIdentifiedVersion conjureIdentifiedVersion) {
        return LockWatchVersion.of(conjureIdentifiedVersion.getId(), conjureIdentifiedVersion.getVersion());
    }
}
