/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

import com.palantir.conjure.java.undertow.annotations.Handle;
import com.palantir.conjure.java.undertow.annotations.HttpMethod;
import com.palantir.conjure.java.undertow.annotations.SerializerFactory;
import com.palantir.conjure.java.undertow.lib.Endpoint;
import com.palantir.conjure.java.undertow.lib.Serializer;
import com.palantir.conjure.java.undertow.lib.TypeMarker;
import com.palantir.conjure.java.undertow.lib.UndertowRuntime;
import com.palantir.logsafe.Safe;
import com.palantir.timestamp.TimestampManagementService;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.OptionalLong;
import javax.annotation.CheckReturnValue;
import javax.annotation.meta.When;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

@Path("/{namespace: (?!(tl|lw)/)[a-zA-Z0-9_-]+}") // Only read by Jersey, not by Undertow
public class TimestampManagementResource {
    private static final long SENTINEL_TIMESTAMP = Long.MIN_VALUE;
    private static final String SENTINEL_TIMESTAMP_STRING =
            SENTINEL_TIMESTAMP + ""; // can't use valueOf/toString because we need a compile time constant!

    private final TimelockNamespaces namespaces;

    public TimestampManagementResource(TimelockNamespaces namespaces) {
        this.namespaces = namespaces;
    }

    // Timestamp management
    @POST
    @Path("timestamp-management/fast-forward")
    @Produces(MediaType.APPLICATION_JSON)
    @Handle(method = HttpMethod.POST, path = "/{namespace}/timestamp-management/fast-forward")
    public void fastForwardTimestamp(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace,
            @Safe
                    @QueryParam("currentTimestamp")
                    @DefaultValue(SENTINEL_TIMESTAMP_STRING)
                    @Handle.QueryParam(value = "currentTimestamp")
                    OptionalLong currentTimestamp,
            @Safe
                    @HeaderParam(TimelockNamespaces.USER_AGENT_HEADER)
                    @Handle.Header(TimelockNamespaces.USER_AGENT_HEADER)
                    Optional<String> userAgent) {
        long timestampToUse = currentTimestamp.orElse(SENTINEL_TIMESTAMP);
        getTimestampManagementService(namespace, userAgent).fastForwardTimestamp(timestampToUse);
    }

    @GET
    @Path("timestamp-management/ping")
    @Produces(MediaType.TEXT_PLAIN)
    @CheckReturnValue(when = When.NEVER)
    @Handle(
            method = HttpMethod.GET,
            path = "/{namespace}/timestamp-management/ping",
            produces = TextPlainSerializer.class)
    public String ping(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace,
            @Safe @Handle.Header(TimelockNamespaces.USER_AGENT_HEADER) Optional<String> userAgent) {
        return getTimestampManagementService(namespace, userAgent).ping();
    }

    private TimestampManagementService getTimestampManagementService(String namespace, Optional<String> userAgent) {
        return namespaces.get(namespace, userAgent).getTimestampManagementService();
    }

    enum TextPlainSerializer implements SerializerFactory<String> {
        INSTANCE;

        @Override
        public <T extends String> Serializer<T> serializer(
                TypeMarker<T> type, UndertowRuntime runtime, Endpoint endpoint) {
            return this::serialize;
        }

        private void serialize(String value, HttpServerExchange exchange) throws IOException {
            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, MediaType.TEXT_PLAIN);
            exchange.getOutputStream().write(value.getBytes(StandardCharsets.UTF_8));
        }
    }
}
