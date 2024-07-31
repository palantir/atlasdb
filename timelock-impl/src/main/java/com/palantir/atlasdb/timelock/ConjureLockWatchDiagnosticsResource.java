/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.http.RedirectRetryTargeter;
import com.palantir.atlasdb.timelock.lock.watch.ConjureLockWatchDiagnosticsService;
import com.palantir.atlasdb.timelock.lock.watch.ConjureLockWatchDiagnosticsServiceEndpoints;
import com.palantir.atlasdb.timelock.lock.watch.UndertowConjureLockWatchDiagnosticsService;
import com.palantir.conjure.java.undertow.lib.RequestContext;
import com.palantir.conjure.java.undertow.lib.UndertowService;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.tokens.auth.AuthHeader;
import java.util.Optional;
import java.util.function.BiFunction;

public final class ConjureLockWatchDiagnosticsResource implements UndertowConjureLockWatchDiagnosticsService {
    private static final SafeLogger log = SafeLoggerFactory.get(ConjureLockWatchDiagnosticsResource.class);
    private final ConjureResourceExceptionHandler exceptionHandler;
    private final BiFunction<String, Optional<String>, AsyncTimelockService> timelockServices;

    private ConjureLockWatchDiagnosticsResource(
            RedirectRetryTargeter redirectRetryTargeter,
            BiFunction<String, Optional<String>, AsyncTimelockService> timelockServices) {
        this.exceptionHandler = new ConjureResourceExceptionHandler(redirectRetryTargeter);
        this.timelockServices = timelockServices;
    }

    public static UndertowService undertow(
            RedirectRetryTargeter redirectRetryTargeter,
            BiFunction<String, Optional<String>, AsyncTimelockService> timelockServices) {
        return ConjureLockWatchDiagnosticsServiceEndpoints.of(
                new ConjureLockWatchDiagnosticsResource(redirectRetryTargeter, timelockServices));
    }

    public static ConjureLockWatchDiagnosticsService jersey(
            RedirectRetryTargeter redirectRetryTargeter,
            BiFunction<String, Optional<String>, AsyncTimelockService> timelockServices) {
        return new JerseyAdapter(new ConjureLockWatchDiagnosticsResource(redirectRetryTargeter, timelockServices));
    }

    @Override
    public ListenableFuture<Void> logState(AuthHeader authHeader, String namespace, RequestContext requestContext) {
        return exceptionHandler.handleExceptions(() -> logStateSync(namespace, requestContext));
    }

    private ListenableFuture<Void> logStateSync(String namespace, RequestContext context) {
        log.info("Logging state for namespace {}", SafeArg.of("namespace", namespace));
        timelockServices
                .apply(namespace, TimelockNamespaces.toUserAgent(context))
                .logState();
        return Futures.immediateFuture(null);
    }

    public static final class JerseyAdapter implements ConjureLockWatchDiagnosticsService {
        private final ConjureLockWatchDiagnosticsResource resource;

        private JerseyAdapter(ConjureLockWatchDiagnosticsResource resource) {
            this.resource = resource;
        }

        @Override
        public void logState(AuthHeader authHeader, String namespace) {
            AtlasFutures.getUnchecked(resource.logState(authHeader, namespace, null));
        }
    }
}
