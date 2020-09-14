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

package com.palantir.timelock.corruption;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.timelock.corruption.TimeLockCorruptionHealthCheck;
import com.palantir.common.remoting.ServiceNotAvailableException;
import com.palantir.conjure.java.undertow.lib.Endpoint;
import com.palantir.conjure.java.undertow.lib.UndertowRuntime;
import com.palantir.conjure.java.undertow.lib.UndertowService;

import io.undertow.server.HandlerWrapper;

public class UndertowCorruptionService implements UndertowService {
    private final UndertowService delegate;
    private final HandlerWrapper wrapper;
    private TimeLockCorruptionHealthCheck healthCheck;


    public UndertowCorruptionService(UndertowService service, TimeLockCorruptionHealthCheck healthCheck) {
        this.delegate = service;
        this.healthCheck = healthCheck;
        this.wrapper = handler -> exchange -> {
            if (healthCheck.isHealthy()) {
                handler.handleRequest(exchange);
            } else {
                throw new ServiceNotAvailableException("TimeLock is not available on account of data corruption.");
            }
        };
    }

    public static UndertowService of(UndertowService service, TimeLockCorruptionHealthCheck healthCheck) {
        return new UndertowCorruptionService(service, healthCheck);
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
