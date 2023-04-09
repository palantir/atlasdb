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
import com.palantir.timestamp.TimestampRange;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import javax.annotation.CheckReturnValue;
import javax.annotation.meta.When;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

/**
 * This class exists as a simple migration of Jersey resources to use conjure-undertow annotations.
 *
 * New endpoints should not be added here; instead, they should be defined in Conjure.
 *
 * Namespaces should respect the regular expression (?!(tl|lw)/)[a-zA-Z0-9_-]+. Requests will still be mapped here, but
 * we do not guarantee we will service them.
 */
public class LegacyAggregatedTimelockResource {
    private static final long SENTINEL_TIMESTAMP = Long.MIN_VALUE;
    private static final String SENTINEL_TIMESTAMP_STRING =
            SENTINEL_TIMESTAMP + ""; // can't use valueOf/toString because we need a compile time constant!
    private static final String PING_RESPONSE = "pong";

    private final TimelockNamespaces namespaces;

    // why did we go for a static factory again?
    private LegacyAggregatedTimelockResource(TimelockNamespaces namespaces) {
        this.namespaces = namespaces;
    }

    public static LegacyAggregatedTimelockResource create(TimelockNamespaces namespaces) {
        return new LegacyAggregatedTimelockResource(namespaces);
    }

    // Legacy timestamp
    @POST // This has to be POST because we can't allow caching.
    @Path("/{namespace}/timestamp/fresh-timestamp")
    @Produces(MediaType.APPLICATION_JSON)
    @Handle(method = HttpMethod.POST, path = "/{namespace}/timestamp/fresh-timestamp")
    public long getFreshTimestamp(@Safe @PathParam("namespace") @Handle.PathParam String namespace) {
        return namespaces.get(namespace).getTimestampService().getFreshTimestamp();
    }

    @POST // This has to be POST because we can't allow caching.
    @Path("fresh-timestamps")
    @Produces(MediaType.APPLICATION_JSON)
    @Handle(method = HttpMethod.POST, path = "/{namespace}/timestamp/fresh-timestamps")
    public TimestampRange getFreshTimestamps(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace,
            @Safe @QueryParam("number") int numTimestampsRequested) {
        return namespaces.get(namespace).getTimestampService().getFreshTimestamps(numTimestampsRequested);
    }

    // Timestamp management
    @POST
    @Path("/{namespace}/timestamp-management/fast-forward")
    @Produces(MediaType.APPLICATION_JSON)
    @Handle(method = HttpMethod.POST, path = "/{namespace}/timestamp-management/fast-forward")
    public void fastForwardTimestamp(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace,
            @Safe
                    @QueryParam("currentTimestamp")
                    @DefaultValue(SENTINEL_TIMESTAMP_STRING)
                    @Handle.QueryParam(value = "currentTimestamp")
                    Long currentTimestamp) {
        long timestampToUse = currentTimestamp == null ? SENTINEL_TIMESTAMP : currentTimestamp;
        namespaces.get(namespace).getTimestampManagementService().fastForwardTimestamp(timestampToUse);
    }

    @GET
    @Path("/{namespace}/timestamp-management/ping")
    @Produces(MediaType.TEXT_PLAIN)
    @CheckReturnValue(when = When.NEVER)
    @Handle(
            method = HttpMethod.POST,
            path = "/{namespace}/timestamp-management/fast-forward",
            produces = TextPlainSerializer.class)
    public String ping(@Safe @PathParam("namespace") @Handle.PathParam String namespace) {
        return namespaces.get(namespace).getTimestampManagementService().ping();
    }

    private enum TextPlainSerializer implements SerializerFactory<String> {
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
