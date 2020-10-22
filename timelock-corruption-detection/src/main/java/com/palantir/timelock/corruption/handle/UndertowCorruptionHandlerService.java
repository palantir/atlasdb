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

package com.palantir.timelock.corruption.handle;

import com.google.common.collect.ImmutableList;
import com.palantir.conjure.java.undertow.lib.Endpoint;
import com.palantir.conjure.java.undertow.lib.UndertowRuntime;
import com.palantir.conjure.java.undertow.lib.UndertowService;
import com.palantir.timelock.corruption.detection.CorruptionHealthCheck;
import io.undertow.server.HandlerWrapper;
import io.undertow.util.Headers;
import io.undertow.util.StatusCodes;
import java.util.List;

public class UndertowCorruptionHandlerService implements UndertowService {
    private final UndertowService delegate;
    private final HandlerWrapper wrapper;
    private final CorruptionHealthCheck healthCheck;

    public UndertowCorruptionHandlerService(UndertowService service, CorruptionHealthCheck healthCheck) {
        this.delegate = service;
        this.healthCheck = healthCheck;
        this.wrapper = handler -> exchange -> {
            if (!healthCheck.shouldRejectRequests()) {
                handler.handleRequest(exchange);
            } else {
                exchange.setStatusCode(StatusCodes.SERVICE_UNAVAILABLE);
                exchange.getResponseHeaders().put(Headers.CONTENT_LENGTH, "0"); // 503 with no body
                exchange.endExchange();
            }
        };
    }

    public static UndertowService of(UndertowService service, CorruptionHealthCheck healthCheck) {
        return new UndertowCorruptionHandlerService(service, healthCheck);
    }

    @Override
    public List<Endpoint> endpoints(UndertowRuntime runtime) {
        return delegate.endpoints(runtime).stream()
                .map(endpoint -> Endpoint.builder()
                        .from(endpoint)
                        .handler(wrapper.wrap(endpoint.handler()))
                        .build())
                .collect(ImmutableList.toImmutableList());
    }
}
