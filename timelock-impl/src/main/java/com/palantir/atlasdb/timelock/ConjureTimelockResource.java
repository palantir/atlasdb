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

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FluentFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.http.RedirectRetryTargeter;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsRequest;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.atlasdb.timelock.api.ConjureTimelockService;
import com.palantir.atlasdb.timelock.api.ConjureTimelockServiceEndpoints;
import com.palantir.atlasdb.timelock.api.UndertowConjureTimelockService;
import com.palantir.conjure.java.api.errors.QosException;
import com.palantir.conjure.java.undertow.lib.UndertowService;
import com.palantir.leader.NotCurrentLeaderException;
import com.palantir.lock.impl.TooManyRequestsException;
import com.palantir.lock.remoting.BlockingTimeoutException;
import com.palantir.lock.v2.ImmutableStartTransactionRequestV5;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.StartTransactionRequestV5;
import com.palantir.lock.v2.StartTransactionResponseV5;
import com.palantir.tokens.auth.AuthHeader;

public final class ConjureTimelockResource implements UndertowConjureTimelockService {
    private final RedirectRetryTargeter redirectRetryTargeter;
    private final Function<String, AsyncTimelockService> timelockServices;

    @VisibleForTesting
    ConjureTimelockResource(
            RedirectRetryTargeter redirectRetryTargeter,
            Function<String, AsyncTimelockService> timelockServices) {
        this.redirectRetryTargeter = redirectRetryTargeter;
        this.timelockServices = timelockServices;
    }

    public static UndertowService undertow(
            RedirectRetryTargeter redirectRetryTargeter,
            Function<String, AsyncTimelockService> timelockServices) {
        return ConjureTimelockServiceEndpoints.of(new ConjureTimelockResource(redirectRetryTargeter, timelockServices));
    }

    public static ConjureTimelockService jersey(
            RedirectRetryTargeter redirectRetryTargeter,
            Function<String, AsyncTimelockService> timelockServices) {
        return new JerseyAdapter(new ConjureTimelockResource(redirectRetryTargeter, timelockServices));
    }

    @Override
    public ListenableFuture<ConjureStartTransactionsResponse> startTransactions(
            AuthHeader authHeader, String namespace, ConjureStartTransactionsRequest request) {
        return handleExceptions(() -> {
            StartTransactionRequestV5 legacyRequest = ImmutableStartTransactionRequestV5.builder()
                    .requestId(request.getRequestId())
                    .requestorId(request.getRequestorId())
                    .numTransactions(request.getNumTransactions())
                    .build();
            StartTransactionResponseV5 response = tl(namespace).startTransactionsWithWatches(legacyRequest);
            return ConjureStartTransactionsResponse.builder()
                    .immutableTimestamp(response.immutableTimestamp())
                    .timestamps(response.timestamps())
                    .lease(response.lease())
                    .build();
        });
    }

    @Override
    public ListenableFuture<LeaderTime> leaderTime(AuthHeader authHeader, String namespace) {
        return handleExceptions(() -> tl(namespace).leaderTime());
    }

    private AsyncTimelockService tl(String namespace) {
        return timelockServices.apply(namespace);
    }

    private <T> ListenableFuture<T> handleExceptions(Supplier<T> supplier) {
        return handleExceptions(Futures.submitAsync(
                () -> Futures.immediateFuture(supplier.get()), MoreExecutors.directExecutor()));
    }

    private <T> ListenableFuture<T> handleExceptions(ListenableFuture<T> future) {
        return FluentFuture.from(future)
                .catching(BlockingTimeoutException.class, timeout -> {
                    throw QosException.throttle(Duration.ZERO);
                }, MoreExecutors.directExecutor())
                .catching(NotCurrentLeaderException.class, notCurrentLeader -> {
                    throw redirectRetryTargeter.redirectRequest(notCurrentLeader.getServiceHint())
                            .<QosException>map(QosException::retryOther)
                            .orElseGet(QosException::unavailable);
                }, MoreExecutors.directExecutor())
                .catching(TooManyRequestsException.class, tooManyRequests -> {
                    throw QosException.throttle();
                }, MoreExecutors.directExecutor());
    }

    private static final class JerseyAdapter implements ConjureTimelockService {
        private final ConjureTimelockResource resource;

        private JerseyAdapter(ConjureTimelockResource resource) {
            this.resource = resource;
        }

        @Override
        public ConjureStartTransactionsResponse startTransactions(
                AuthHeader authHeader,
                String namespace,
                ConjureStartTransactionsRequest request) {
            return unwrap(resource.startTransactions(authHeader, namespace, request));
        }

        @Override
        public LeaderTime leaderTime(AuthHeader authHeader, String namespace) {
            return unwrap(resource.leaderTime(authHeader, namespace));
        }

        private static <T> T unwrap(ListenableFuture<T> future) {
            try {
                return future.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw Throwables.propagate(e);
            }
        }
    }
}
