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

package com.palantir.atlasdb.timelock.lock.v1;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.http.RedirectRetryTargeter;
import com.palantir.atlasdb.timelock.ConjureResourceExceptionHandler;
import com.palantir.conjure.java.undertow.lib.UndertowService;
import com.palantir.lock.ConjureLockRefreshToken;
import com.palantir.lock.ConjureLockV1Request;
import com.palantir.lock.ConjureLockV1ServiceEndpoints;
import com.palantir.lock.ConjureSimpleHeldLocksToken;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockService;
import com.palantir.lock.SimpleHeldLocksToken;
import com.palantir.lock.UndertowConjureLockV1Service;
import com.palantir.lock.client.ConjureLockV1Tokens;
import com.palantir.tokens.auth.AuthHeader;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

public final class ConjureLockV1Resource implements UndertowConjureLockV1Service {
    private final ConjureResourceExceptionHandler exceptionHandler;
    private final Function<String, LockService> lockServices;

    private ConjureLockV1Resource(
            RedirectRetryTargeter redirectRetryTargeter, Function<String, LockService> lockServices) {
        this.exceptionHandler = new ConjureResourceExceptionHandler(redirectRetryTargeter);
        this.lockServices = lockServices;
    }

    public static UndertowService undertow(
            RedirectRetryTargeter redirectRetryTargeter, Function<String, LockService> lockServices) {
        return ConjureLockV1ServiceEndpoints.of(new ConjureLockV1Resource(redirectRetryTargeter, lockServices));
    }

    public static ConjureLockV1ShimService jersey(
            RedirectRetryTargeter redirectRetryTargeter, Function<String, LockService> lockServices) {
        return new ConjureLockV1Resource.JerseyAdapter(new ConjureLockV1Resource(redirectRetryTargeter, lockServices));
    }

    @Override
    public ListenableFuture<Optional<HeldLocksToken>> lockAndGetHeldLocks(
            AuthHeader _authHeader, String namespace, ConjureLockV1Request request) {
        return exceptionHandler.handleExceptions(() -> {
            try {
                return Futures.immediateFuture(Optional.ofNullable(lockServices
                        .apply(namespace)
                        .lockAndGetHeldLocks(request.getLockClient(), request.getLockRequest())));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public ListenableFuture<Set<ConjureLockRefreshToken>> refreshLockRefreshTokens(
            AuthHeader _authHeader, String namespace, List<ConjureLockRefreshToken> request) {
        return exceptionHandler.handleExceptions(() -> {
            ListenableFuture<Set<LockRefreshToken>> serviceTokens = Futures.immediateFuture(lockServices
                    .apply(namespace)
                    .refreshLockRefreshTokens(ConjureLockV1Tokens.getLegacyTokens(request)));
            return Futures.transform(
                    serviceTokens,
                    tokens -> ImmutableSet.copyOf(ConjureLockV1Tokens.getConjureTokens(tokens)),
                    MoreExecutors.directExecutor());
        });
    }

    @Override
    public ListenableFuture<Boolean> unlockSimple(
            AuthHeader _authHeader, String namespace, ConjureSimpleHeldLocksToken request) {
        return exceptionHandler.handleExceptions(() -> {
            SimpleHeldLocksToken serverToken =
                    new SimpleHeldLocksToken(request.getTokenId(), request.getCreationDateMs());
            return Futures.immediateFuture(lockServices.apply(namespace).unlockSimple(serverToken));
        });
    }

    public static final class JerseyAdapter implements ConjureLockV1ShimService {
        private final ConjureLockV1Resource resource;

        private JerseyAdapter(ConjureLockV1Resource resource) {
            this.resource = resource;
        }

        @Override
        public Optional<HeldLocksToken> lockAndGetHeldLocks(
                AuthHeader authHeader, String namespace, ConjureLockV1Request request) {
            return unwrap(resource.lockAndGetHeldLocks(authHeader, namespace, request));
        }

        @Override
        public Set<ConjureLockRefreshToken> refreshLockRefreshTokens(
                AuthHeader authHeader, String namespace, List<ConjureLockRefreshToken> request) {
            return unwrap(resource.refreshLockRefreshTokens(authHeader, namespace, request));
        }

        @Override
        public boolean unlockSimple(AuthHeader authHeader, String namespace, ConjureSimpleHeldLocksToken request) {
            return unwrap(resource.unlockSimple(authHeader, namespace, request));
        }

        private static <T> T unwrap(ListenableFuture<T> future) {
            return AtlasFutures.getUnchecked(future);
        }
    }
}
