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

import com.palantir.annotations.remoting.CancelableServerCall;
import com.palantir.common.annotation.Idempotent;
import com.palantir.common.annotation.NonIdempotent;
import com.palantir.conjure.java.undertow.annotations.Handle;
import com.palantir.conjure.java.undertow.annotations.HttpMethod;
import com.palantir.conjure.java.undertow.annotations.SerializerFactory;
import com.palantir.conjure.java.undertow.lib.Endpoint;
import com.palantir.conjure.java.undertow.lib.Serializer;
import com.palantir.conjure.java.undertow.lib.TypeMarker;
import com.palantir.conjure.java.undertow.lib.UndertowRuntime;
import com.palantir.lock.HeldLocksGrant;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockResponse;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.SimpleHeldLocksToken;
import com.palantir.logsafe.Safe;
import com.palantir.timestamp.TimestampRange;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.meta.When;
import javax.ws.rs.Consumes;
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
    @Path("/{namespace}/timestamp/fresh-timestamps")
    @Produces(MediaType.APPLICATION_JSON)
    @Handle(method = HttpMethod.POST, path = "/{namespace}/timestamp/fresh-timestamps")
    public TimestampRange getFreshTimestamps(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace,
            @Safe @QueryParam("number") int numTimestampsRequested) {
        return namespaces.get(namespace).getTimestampService().getFreshTimestamps(numTimestampsRequested);
    }

    // Lock v1
    @POST
    @Path("/{namespace}/lock/lock-with-full-response/{client}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @CancelableServerCall
    @NonIdempotent
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/lock-with-full-response/{client}")
    public LockResponse lockWithFullLockResponse(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace,
            @Safe @PathParam("client") @Handle.PathParam LockClient client,
            @Handle.Body LockRequest request)
            throws InterruptedException {
        return namespaces.get(namespace).getLockService().lockWithFullLockResponse(client, request);
    }

    @POST
    @Path("/{namespace}/lock/unlock-deprecated")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Deprecated
    @NonIdempotent
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/unlock-deprecated")
    public boolean unlock(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace, @Handle.Body HeldLocksToken token) {
        return namespaces.get(namespace).getLockService().unlock(token);
    }

    @POST
    @Path("/{namespace}/lock/unlock-simple")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @NonIdempotent
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/unlock-simple")
    public boolean unlockSimple(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace, @Handle.Body SimpleHeldLocksToken token) {
        return namespaces.get(namespace).getLockService().unlockSimple(token);
    }

    @POST
    @Path("/{namespace}/lock/unlock-and-freeze")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @NonIdempotent
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/unlock-and-freeze")
    public boolean unlockAndFreeze(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace, @Handle.Body HeldLocksToken token) {
        return namespaces.get(namespace).getLockService().unlockAndFreeze(token);
    }

    @POST
    @Path("/{namespace}/lock/get-tokens/{client}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @CancelableServerCall
    @NonIdempotent
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/get-tokens/{client}")
    public Set<HeldLocksToken> getTokens(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace,
            @Safe @PathParam("client") @Handle.PathParam LockClient client) {
        return namespaces.get(namespace).getLockService().getTokens(client);
    }

    @POST
    @Path("/{namespace}/lock/refresh-tokens")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Deprecated
    @Idempotent
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/refresh-tokens")
    public Set<HeldLocksToken> refreshTokens(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace,
            @Handle.Body Iterable<HeldLocksToken> tokens) {
        return namespaces.get(namespace).getLockService().refreshTokens(tokens);
    }

    @POST
    @Path("/{namespace}/lock/refresh-grant")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Idempotent
    @Nullable
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/refresh-grant")
    public HeldLocksGrant refreshGrant(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace, @Handle.Body HeldLocksGrant grant) {
        return namespaces.get(namespace).getLockService().refreshGrant(grant);
    }

    @POST
    @Path("/{namespace}/lock/refresh-grant-id")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Idempotent
    @Nullable
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/refresh-grant-id")
    public HeldLocksGrant refreshGrant(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace, @Handle.Body BigInteger grantId) {
        return namespaces.get(namespace).getLockService().refreshGrant(grantId);
    }

    @POST
    @Path("/{namespace}/lock/convert-to-grant")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @NonIdempotent
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/convert-to-grant")
    public HeldLocksGrant convertToGrant(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace, @Handle.Body HeldLocksToken token) {
        return namespaces.get(namespace).getLockService().convertToGrant(token);
    }

    @POST
    @Path("/{namespace}/lock/use-grant/{client}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @NonIdempotent
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/use-grant/{client}")
    public HeldLocksToken useGrant(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace,
            @Safe @PathParam("client") @Handle.PathParam LockClient client,
            @Handle.Body HeldLocksGrant grant) {
        return namespaces.get(namespace).getLockService().useGrant(client, grant);
    }

    @POST
    @Path("/{namespace}/lock/use-grant-id/{client}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @NonIdempotent
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/use-grant-id/{client}")
    public HeldLocksToken useGrant(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace,
            @Safe @PathParam("client") @Handle.PathParam LockClient client,
            @Handle.Body BigInteger grantId) {
        return namespaces.get(namespace).getLockService().useGrant(client, grantId);
    }

    @POST
    @Path("/{namespace}/lock/min-locked-in-version-id")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Deprecated
    @Idempotent
    @Nullable
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/min-locked-in-version-id")
    public Long getMinLockedInVersionId(@Safe @PathParam("namespace") @Handle.PathParam String namespace) {
        return namespaces.get(namespace).getLockService().getMinLockedInVersionId(namespace);
    }

    @POST
    @Path("/{namespace}/lock/min-locked-in-version-id-for-client/{client}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/min-locked-in-version-id-for-client/{client}")
    public Long getMinLockedInVersionId(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace,
            @Safe @PathParam("client") @Handle.PathParam LockClient client) {
        return namespaces.get(namespace).getLockService().getMinLockedInVersionId(client);
    }

    @POST
    @Path("/{namespace}/lock/lock-server-options")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Idempotent
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/lock-server-options")
    public LockServerOptions getLockServerOptions(@Safe @PathParam("namespace") @Handle.PathParam String namespace) {
        return namespaces.get(namespace).getLockService().getLockServerOptions();
    }

    @POST
    @Path("/{namespace}/lock/current-time-millis")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Idempotent
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/current-time-millis")
    public long currentTimeMillis(@Safe @PathParam("namespace") @Handle.PathParam String namespace) {
        return namespaces.get(namespace).getLockService().currentTimeMillis();
    }

    @POST
    @Path("/{namespace}/lock/log-current-state")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Idempotent
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/log-current-state")
    public void logCurrentState(@Safe @PathParam("namespace") @Handle.PathParam String namespace) {
        namespaces.get(namespace).getLockService().logCurrentState();
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
