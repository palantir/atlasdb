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
import com.palantir.atlasdb.backup.AuthHeaderValidator;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.http.RedirectRetryTargeter;
import com.palantir.atlasdb.timelock.ConjureResourceExceptionHandler;
import com.palantir.atlasdb.timelock.TimelockNamespaces;
import com.palantir.atlasdb.timelock.api.DisableNamespacesRequest;
import com.palantir.atlasdb.timelock.api.DisabledNamespacesUpdaterService;
import com.palantir.atlasdb.timelock.api.DisabledNamespacesUpdaterServiceEndpoints;
import com.palantir.atlasdb.timelock.api.ReenableNamespacesRequest;
import com.palantir.atlasdb.timelock.api.SingleNodeUpdateResponse;
import com.palantir.atlasdb.timelock.api.UndertowDisabledNamespacesUpdaterService;
import com.palantir.conjure.java.api.errors.ErrorType;
import com.palantir.conjure.java.api.errors.ServiceException;
import com.palantir.conjure.java.undertow.lib.UndertowService;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.tokens.auth.AuthHeader;
import java.util.function.Supplier;

public final class DisabledNamespacesUpdaterResource implements UndertowDisabledNamespacesUpdaterService {
    private static final SafeLogger log = SafeLoggerFactory.get(DisabledNamespacesUpdaterResource.class);

    private final AuthHeaderValidator authHeaderValidator;
    private final ConjureResourceExceptionHandler exceptionHandler;
    private final TimelockNamespaces timelockNamespaces;

    private DisabledNamespacesUpdaterResource(
            AuthHeaderValidator authHeaderValidator,
            RedirectRetryTargeter redirectRetryTargeter,
            TimelockNamespaces timelockNamespaces) {
        this.authHeaderValidator = authHeaderValidator;
        this.exceptionHandler = new ConjureResourceExceptionHandler(redirectRetryTargeter);
        this.timelockNamespaces = timelockNamespaces;
    }

    public static UndertowService undertow(
            AuthHeaderValidator authHeaderValidator,
            RedirectRetryTargeter redirectRetryTargeter,
            TimelockNamespaces timelockNamespaces) {
        return DisabledNamespacesUpdaterServiceEndpoints.of(
                new DisabledNamespacesUpdaterResource(authHeaderValidator, redirectRetryTargeter, timelockNamespaces));
    }

    public static DisabledNamespacesUpdaterService jersey(
            AuthHeaderValidator authHeaderValidator,
            RedirectRetryTargeter redirectRetryTargeter,
            TimelockNamespaces timelockNamespaces) {
        return new JerseyAdapter(
                new DisabledNamespacesUpdaterResource(authHeaderValidator, redirectRetryTargeter, timelockNamespaces));
    }

    @Override
    public ListenableFuture<Boolean> ping(AuthHeader _authHeader) {
        return Futures.immediateFuture(true);
    }

    @Override
    public ListenableFuture<SingleNodeUpdateResponse> disable(AuthHeader authHeader, DisableNamespacesRequest request) {
        if (!authHeaderValidator.suppliedTokenIsValid(authHeader)) {
            log.error(
                    "Attempted to disable TimeLock with an invalid auth header. "
                            + "The provided token must match the configured permitted-backup-token.",
                    SafeArg.of("request", request));
            throw new ServiceException(ErrorType.PERMISSION_DENIED);
        }
        return handleExceptions(() -> Futures.immediateFuture(timelockNamespaces.disable(request)));
    }

    @Override
    public ListenableFuture<SingleNodeUpdateResponse> reenable(
            AuthHeader authHeader, ReenableNamespacesRequest request) {
        if (!authHeaderValidator.suppliedTokenIsValid(authHeader)) {
            log.error(
                    "Attempted to re-enable TimeLock with an invalid auth header. "
                            + "The provided token must match the configured permitted-backup-token.",
                    SafeArg.of("request", request));
            throw new ServiceException(ErrorType.PERMISSION_DENIED);
        }
        return handleExceptions(() -> Futures.immediateFuture(timelockNamespaces.reEnable(request)));
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
        public SingleNodeUpdateResponse disable(AuthHeader authHeader, DisableNamespacesRequest request) {
            return unwrap(resource.disable(authHeader, request));
        }

        @Override
        public SingleNodeUpdateResponse reenable(AuthHeader authHeader, ReenableNamespacesRequest request) {
            return unwrap(resource.reenable(authHeader, request));
        }

        private static <T> T unwrap(ListenableFuture<T> future) {
            return AtlasFutures.getUnchecked(future);
        }
    }
}
