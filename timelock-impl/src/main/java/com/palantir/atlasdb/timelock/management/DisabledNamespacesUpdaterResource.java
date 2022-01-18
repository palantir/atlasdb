/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.management;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.http.RedirectRetryTargeter;
import com.palantir.atlasdb.timelock.ConjureResourceExceptionHandler;
import com.palantir.atlasdb.timelock.api.DisableNamespacesRequest;
import com.palantir.atlasdb.timelock.api.DisableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.DisabledNamespacesUpdaterService;
import com.palantir.atlasdb.timelock.api.DisabledNamespacesUpdaterServiceEndpoints;
import com.palantir.atlasdb.timelock.api.ReenableNamespacesRequest;
import com.palantir.atlasdb.timelock.api.ReenableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.UndertowDisabledNamespacesUpdaterService;
import com.palantir.conjure.java.undertow.lib.UndertowService;
import com.palantir.tokens.auth.AuthHeader;
import java.util.function.Supplier;

public class DisabledNamespacesUpdaterResource implements UndertowDisabledNamespacesUpdaterService {
    private final ConjureResourceExceptionHandler exceptionHandler;
    private final DisabledNamespaces disabledNamespaces;

    private DisabledNamespacesUpdaterResource(
            RedirectRetryTargeter redirectRetryTargeter, DisabledNamespaces disabledNamespaces) {
        this.exceptionHandler = new ConjureResourceExceptionHandler(redirectRetryTargeter);
        this.disabledNamespaces = disabledNamespaces;
    }

    public static UndertowService undertow(
            RedirectRetryTargeter redirectRetryTargeter, DisabledNamespaces disabledNamespaces) {
        return DisabledNamespacesUpdaterServiceEndpoints.of(
                new DisabledNamespacesUpdaterResource(redirectRetryTargeter, disabledNamespaces));
    }

    public static DisabledNamespacesUpdaterService jersey(
            RedirectRetryTargeter redirectRetryTargeter, DisabledNamespaces disabledNamespaces) {
        return new JerseyAdapter(new DisabledNamespacesUpdaterResource(redirectRetryTargeter, disabledNamespaces));
    }

    @Override
    public ListenableFuture<Boolean> ping(AuthHeader _authHeader) {
        return Futures.immediateFuture(true);
    }

    @Override
    public ListenableFuture<DisableNamespacesResponse> disable(
            AuthHeader authHeader, DisableNamespacesRequest request) {
        return handleExceptions(() -> Futures.immediateFuture(disabledNamespaces.disable(request.getNamespaces())));
    }

    @Override
    public ListenableFuture<ReenableNamespacesResponse> reenable(
            AuthHeader authHeader, ReenableNamespacesRequest request) {
        return handleExceptions(() -> Futures.immediateFuture(disabledNamespaces.reEnable(request)));
    }

    private <T> ListenableFuture<T> handleExceptions(Supplier<ListenableFuture<T>> supplier) {
        return exceptionHandler.handleExceptions(supplier);
    }

    public static final class JerseyAdapter implements DisabledNamespacesUpdaterService {
        private final DisabledNamespacesUpdaterResource resource;

        public JerseyAdapter(DisabledNamespacesUpdaterResource resource) {
            this.resource = resource;
        }

        @Override
        public boolean ping(AuthHeader authHeader) {
            return unwrap(resource.ping(authHeader));
        }

        @Override
        public DisableNamespacesResponse disable(AuthHeader authHeader, DisableNamespacesRequest request) {
            return unwrap(resource.disable(authHeader, request));
        }

        @Override
        public ReenableNamespacesResponse reenable(AuthHeader authHeader, ReenableNamespacesRequest request) {
            return unwrap(resource.reenable(authHeader, request));
        }

        private static <T> T unwrap(ListenableFuture<T> future) {
            return AtlasFutures.getUnchecked(future);
        }
    }
}
