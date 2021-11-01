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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.http.RedirectRetryTargeter;
import com.palantir.atlasdb.timelock.api.LockWatchRequest;
import com.palantir.atlasdb.timelock.lock.watch.ConjureLockWatchingService;
import com.palantir.atlasdb.timelock.lock.watch.ConjureLockWatchingServiceEndpoints;
import com.palantir.atlasdb.timelock.lock.watch.UndertowConjureLockWatchingService;
import com.palantir.conjure.java.undertow.lib.UndertowService;
import com.palantir.tokens.auth.AuthHeader;
import java.util.function.Function;

public final class ConjureLockWatchingResource implements UndertowConjureLockWatchingService {
    private final ConjureResourceExceptionHandler exceptionHandler;
    private final Function<String, AsyncTimelockService> timelockServices;

    private ConjureLockWatchingResource(
            RedirectRetryTargeter redirectRetryTargeter, Function<String, AsyncTimelockService> timelockServices) {
        this.exceptionHandler = new ConjureResourceExceptionHandler(redirectRetryTargeter);
        this.timelockServices = timelockServices;
    }

    public static UndertowService undertow(
            RedirectRetryTargeter redirectRetryTargeter, Function<String, AsyncTimelockService> timelockServices) {
        return ConjureLockWatchingServiceEndpoints.of(
                new ConjureLockWatchingResource(redirectRetryTargeter, timelockServices));
    }

    public static ConjureLockWatchingService jersey(
            RedirectRetryTargeter redirectRetryTargeter, Function<String, AsyncTimelockService> timelockServices) {
        return new JerseyAdapter(new ConjureLockWatchingResource(redirectRetryTargeter, timelockServices));
    }

    @Override
    public ListenableFuture<Void> startWatching(AuthHeader authHeader, String namespace, LockWatchRequest request) {
        return exceptionHandler.handleExceptions(() -> startWatchingSync(namespace, request));
    }

    private ListenableFuture<Void> startWatchingSync(String namespace, LockWatchRequest request) {
        timelockServices.apply(namespace).startWatching(request);
        return Futures.immediateFuture(null);
    }

    public static final class JerseyAdapter implements ConjureLockWatchingService {
        private final ConjureLockWatchingResource resource;

        private JerseyAdapter(ConjureLockWatchingResource resource) {
            this.resource = resource;
        }

        @Override
        public void startWatching(AuthHeader authHeader, String namespace, LockWatchRequest request) {
            AtlasFutures.getUnchecked(resource.startWatching(authHeader, namespace, request));
        }
    }
}
