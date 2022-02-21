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

package com.palantir.timelock.paxos;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.http.AtlasDbHttpProtocolVersion;
import com.palantir.atlasdb.http.ProtocolAwareness;
import com.palantir.atlasdb.http.RedirectRetryTargeter;
import com.palantir.conjure.java.api.errors.QosException;
import com.palantir.conjure.java.undertow.lib.Endpoint;
import com.palantir.conjure.java.undertow.lib.UndertowRuntime;
import com.palantir.conjure.java.undertow.lib.UndertowService;
import com.palantir.leader.NotCurrentLeaderException;
import com.palantir.lock.impl.TooManyRequestsException;
import com.palantir.lock.remoting.BlockingTimeoutException;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import java.time.Duration;
import java.util.List;
import java.util.function.Function;

final class TimelockUndertowExceptionWrapper implements UndertowService {

    private final UndertowService delegate;
    private final RedirectRetryTargeter redirectRetryTargeter;

    TimelockUndertowExceptionWrapper(UndertowService delegate, RedirectRetryTargeter redirectRetryTargeter) {
        this.delegate = delegate;
        this.redirectRetryTargeter = redirectRetryTargeter;
    }

    @Override
    public List<Endpoint> endpoints(UndertowRuntime runtime) {
        return delegate.endpoints(runtime).stream()
                .map(endpoint -> Endpoint.builder()
                        .from(endpoint)
                        .handler(new TimelockHandler(endpoint.handler(), redirectRetryTargeter))
                        .build())
                .collect(ImmutableList.toImmutableList());
    }

    private static final class TimelockHandler implements HttpHandler {
        private static final SafeLogger log = SafeLoggerFactory.get(TimelockHandler.class);
        private final HttpHandler delegate;
        private final RedirectRetryTargeter redirectTargeter;

        TimelockHandler(HttpHandler delegate, RedirectRetryTargeter redirectTargeter) {
            this.delegate = delegate;
            this.redirectTargeter = redirectTargeter;
        }

        @Override
        public void handleRequest(HttpServerExchange exchange) throws Exception {
            try {
                delegate.handleRequest(exchange);
            } catch (TooManyRequestsException tooManyRequestsException) {
                throw QosException.throttle(tooManyRequestsException);
            } catch (BlockingTimeoutException blockingTimeoutException) {
                throw forVersion(
                        exchange,
                        blockingTimeoutException,
                        QosException::unavailable,
                        exception -> QosException.throttle(Duration.ZERO, exception));
            } catch (NotCurrentLeaderException notCurrentLeaderException) {
                throw forVersion(
                        exchange, notCurrentLeaderException, QosException::unavailable, exception -> redirectTargeter
                                .redirectRequest(exception.getServiceHint())
                                .<QosException>map(redirectTarget ->
                                        QosException.retryOther(redirectTarget, notCurrentLeaderException))
                                .orElseGet(() -> QosException.unavailable(notCurrentLeaderException)));
            }
        }

        private static <T, E> T forVersion(
                HttpServerExchange exchange, E exception, Function<E, T> legacy, Function<E, T> cjr) {
            AtlasDbHttpProtocolVersion version = ProtocolAwareness.getHttpProtocolVersion(
                    exchange.getRequestHeaders().get(Headers.USER_AGENT));
            switch (version) {
                case LEGACY_OR_UNKNOWN:
                    return legacy.apply(exception);
                case CONJURE_JAVA_RUNTIME:
                    return cjr.apply(exception);
                default:
                    log.warn(
                            "Couldn't determine what to do with protocol version {}. This is a product bug.",
                            SafeArg.of("protocolVersion", version));
                    throw new SafeIllegalStateException(
                            "Unrecognized protocol version in HttpProtocolAwareExceptionMapper",
                            SafeArg.of("protocolVersion", version));
            }
        }
    }
}
