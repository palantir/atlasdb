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
import com.palantir.lock.HeldLocksGrant;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockResponse;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.LockService;
import com.palantir.lock.LockState;
import com.palantir.lock.SimpleHeldLocksToken;
import com.palantir.logsafe.Safe;
import java.math.BigInteger;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
import javax.ws.rs.Consumes;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/{namespace: (?!(tl|lw)/)[a-zA-Z0-9_-]+}") // Only read by Jersey, not by Undertow
public class LockV1Resource {
    private final TimelockNamespaces namespaces;

    public LockV1Resource(TimelockNamespaces namespaces) {
        this.namespaces = namespaces;
    }

    // Lock v1
    @POST
    @Path("/lock/lock-with-full-response/{client}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @CancelableServerCall
    @NonIdempotent
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/lock-with-full-response/{client}")
    public Optional<LockResponse> lockWithFullLockResponse(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace,
            @Safe @PathParam("client") @Handle.PathParam LockClient client,
            @Handle.Body LockRequest request,
            @Safe
                    @HeaderParam(TimelockNamespaces.USER_AGENT_HEADER)
                    @Handle.Header(TimelockNamespaces.USER_AGENT_HEADER)
                    Optional<String> userAgent)
            throws InterruptedException {
        return Optional.ofNullable(getLockService(namespace, userAgent).lockWithFullLockResponse(client, request));
    }

    @POST
    @Path("/lock/lock-with-full-response")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @CancelableServerCall
    @NonIdempotent
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/lock-with-full-response")
    public Optional<LockResponse> lockWithFullLockResponse(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace,
            @Handle.Body LockRequest request,
            @Safe
                    @HeaderParam(TimelockNamespaces.USER_AGENT_HEADER)
                    @Handle.Header(TimelockNamespaces.USER_AGENT_HEADER)
                    Optional<String> userAgent)
            throws InterruptedException {
        return lockWithFullLockResponse(namespace, LockClient.ANONYMOUS, request, userAgent);
    }

    @POST
    @Path("lock/unlock-deprecated")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Deprecated
    @NonIdempotent
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/unlock-deprecated")
    public boolean unlock(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace,
            @Handle.Body HeldLocksToken token,
            @Safe
                    @HeaderParam(TimelockNamespaces.USER_AGENT_HEADER)
                    @Handle.Header(TimelockNamespaces.USER_AGENT_HEADER)
                    Optional<String> userAgent) {
        return getLockService(namespace, userAgent).unlock(token);
    }

    @POST
    @Path("lock/unlock-simple")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @NonIdempotent
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/unlock-simple")
    public boolean unlockSimple(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace,
            @Handle.Body SimpleHeldLocksToken token,
            @Safe
                    @HeaderParam(TimelockNamespaces.USER_AGENT_HEADER)
                    @Handle.Header(TimelockNamespaces.USER_AGENT_HEADER)
                    Optional<String> userAgent) {
        return getLockService(namespace, userAgent).unlockSimple(token);
    }

    @POST
    @Path("lock/unlock-and-freeze")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @NonIdempotent
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/unlock-and-freeze")
    public boolean unlockAndFreeze(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace,
            @Handle.Body HeldLocksToken token,
            @Safe
                    @HeaderParam(TimelockNamespaces.USER_AGENT_HEADER)
                    @Handle.Header(TimelockNamespaces.USER_AGENT_HEADER)
                    Optional<String> userAgent) {
        return getLockService(namespace, userAgent).unlockAndFreeze(token);
    }

    @POST
    @Path("lock/get-tokens/{client}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Idempotent
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/get-tokens/{client}")
    public Set<HeldLocksToken> getTokens(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace,
            @Safe @PathParam("client") @Handle.PathParam LockClient client,
            @Safe
                    @HeaderParam(TimelockNamespaces.USER_AGENT_HEADER)
                    @Handle.Header(TimelockNamespaces.USER_AGENT_HEADER)
                    Optional<String> userAgent) {
        return getLockService(namespace, userAgent).getTokens(client);
    }

    @POST
    @Path("lock/get-tokens")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Idempotent
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/get-tokens")
    public Set<HeldLocksToken> getTokens(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace,
            @Safe
                    @HeaderParam(TimelockNamespaces.USER_AGENT_HEADER)
                    @Handle.Header(TimelockNamespaces.USER_AGENT_HEADER)
                    Optional<String> userAgent) {
        return getTokens(namespace, LockClient.ANONYMOUS, userAgent);
    }

    @POST
    @Path("lock/refresh-tokens")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Deprecated
    @Idempotent
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/refresh-tokens")
    public Set<HeldLocksToken> refreshTokens(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace,
            @Handle.Body Iterable<HeldLocksToken> tokens,
            @Safe
                    @HeaderParam(TimelockNamespaces.USER_AGENT_HEADER)
                    @Handle.Header(TimelockNamespaces.USER_AGENT_HEADER)
                    Optional<String> userAgent) {
        return getLockService(namespace, userAgent).refreshTokens(tokens);
    }

    @POST
    @Path("lock/refresh-grant")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Idempotent
    @Nullable
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/refresh-grant")
    @SuppressWarnings("NullableOptional") // null and empty are same over the wire
    public Optional<HeldLocksGrant> refreshGrant(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace,
            @Handle.Body HeldLocksGrant grant,
            @Safe
                    @HeaderParam(TimelockNamespaces.USER_AGENT_HEADER)
                    @Handle.Header(TimelockNamespaces.USER_AGENT_HEADER)
                    Optional<String> userAgent) {
        return Optional.ofNullable(getLockService(namespace, userAgent).refreshGrant(grant));
    }

    @POST
    @Path("lock/refresh-grant-id")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Idempotent
    @Nullable
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/refresh-grant-id")
    @SuppressWarnings("NullableOptional") // null and empty are same over the wire
    public Optional<HeldLocksGrant> refreshGrant(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace,
            @Handle.Body BigInteger grantId,
            @Safe
                    @HeaderParam(TimelockNamespaces.USER_AGENT_HEADER)
                    @Handle.Header(TimelockNamespaces.USER_AGENT_HEADER)
                    Optional<String> userAgent) {
        return Optional.ofNullable(getLockService(namespace, userAgent).refreshGrant(grantId));
    }

    @POST
    @Path("lock/convert-to-grant")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @NonIdempotent
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/convert-to-grant")
    public HeldLocksGrant convertToGrant(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace,
            @Handle.Body HeldLocksToken token,
            @Safe
                    @HeaderParam(TimelockNamespaces.USER_AGENT_HEADER)
                    @Handle.Header(TimelockNamespaces.USER_AGENT_HEADER)
                    Optional<String> userAgent) {
        return getLockService(namespace, userAgent).convertToGrant(token);
    }

    @POST
    @Path("lock/use-grant/{client}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @NonIdempotent
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/use-grant/{client}")
    public HeldLocksToken useGrant(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace,
            @Safe @PathParam("client") @Handle.PathParam LockClient client,
            @Handle.Body HeldLocksGrant grant,
            @Safe
                    @HeaderParam(TimelockNamespaces.USER_AGENT_HEADER)
                    @Handle.Header(TimelockNamespaces.USER_AGENT_HEADER)
                    Optional<String> userAgent) {
        return getLockService(namespace, userAgent).useGrant(client, grant);
    }

    @POST
    @Path("lock/use-grant")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @NonIdempotent
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/use-grant")
    public HeldLocksToken useGrant(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace,
            @Handle.Body HeldLocksGrant grant,
            @Safe
                    @HeaderParam(TimelockNamespaces.USER_AGENT_HEADER)
                    @Handle.Header(TimelockNamespaces.USER_AGENT_HEADER)
                    Optional<String> userAgent) {
        return useGrant(namespace, LockClient.ANONYMOUS, grant, userAgent);
    }

    @POST
    @Path("lock/use-grant-id/{client}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @NonIdempotent
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/use-grant-id/{client}")
    public HeldLocksToken useGrant(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace,
            @Safe @PathParam("client") @Handle.PathParam LockClient client,
            @Handle.Body BigInteger grantId,
            @Safe
                    @HeaderParam(TimelockNamespaces.USER_AGENT_HEADER)
                    @Handle.Header(TimelockNamespaces.USER_AGENT_HEADER)
                    Optional<String> userAgent) {
        return getLockService(namespace, userAgent).useGrant(client, grantId);
    }

    @POST
    @Path("lock/use-grant-id")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @NonIdempotent
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/use-grant-id")
    public HeldLocksToken useGrant(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace,
            @Handle.Body BigInteger grantId,
            @Safe
                    @HeaderParam(TimelockNamespaces.USER_AGENT_HEADER)
                    @Handle.Header(TimelockNamespaces.USER_AGENT_HEADER)
                    Optional<String> userAgent) {
        return useGrant(namespace, LockClient.ANONYMOUS, grantId, userAgent);
    }

    @POST
    @Path("lock/min-locked-in-version-id")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Deprecated
    @Idempotent
    @Nullable
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/min-locked-in-version-id")
    @SuppressWarnings("NullableOptional") // null and empty are same over the wire
    public Optional<Long> getMinLockedInVersionId(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace,
            @Safe
                    @HeaderParam(TimelockNamespaces.USER_AGENT_HEADER)
                    @Handle.Header(TimelockNamespaces.USER_AGENT_HEADER)
                    Optional<String> userAgent) {
        return Optional.ofNullable(getLockService(namespace, userAgent).getMinLockedInVersionId());
    }

    @POST
    @Path("lock/min-locked-in-version-id-for-client/{client}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/min-locked-in-version-id-for-client/{client}")
    public Optional<Long> getMinLockedInVersionId(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace,
            @Safe @PathParam("client") @Handle.PathParam LockClient client,
            @Safe
                    @HeaderParam(TimelockNamespaces.USER_AGENT_HEADER)
                    @Handle.Header(TimelockNamespaces.USER_AGENT_HEADER)
                    Optional<String> userAgent) {
        return Optional.ofNullable(getLockService(namespace, userAgent).getMinLockedInVersionId(client));
    }

    @POST
    @Path("lock/min-locked-in-version-id-for-client")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/min-locked-in-version-id-for-client")
    public Optional<Long> getMinLockedInVersionIdForAnonymousClient(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace,
            @Safe
                    @HeaderParam(TimelockNamespaces.USER_AGENT_HEADER)
                    @Handle.Header(TimelockNamespaces.USER_AGENT_HEADER)
                    Optional<String> userAgent) {
        return getMinLockedInVersionId(namespace, LockClient.ANONYMOUS, userAgent);
    }

    @POST
    @Path("lock/lock-server-options")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Idempotent
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/lock-server-options")
    public LockServerOptions getLockServerOptions(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace,
            @Safe
                    @HeaderParam(TimelockNamespaces.USER_AGENT_HEADER)
                    @Handle.Header(TimelockNamespaces.USER_AGENT_HEADER)
                    Optional<String> userAgent) {
        return getLockService(namespace, userAgent).getLockServerOptions();
    }

    @POST
    @Path("lock/current-time-millis")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Idempotent
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/current-time-millis")
    public long currentTimeMillis(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace,
            @Safe
                    @HeaderParam(TimelockNamespaces.USER_AGENT_HEADER)
                    @Handle.Header(TimelockNamespaces.USER_AGENT_HEADER)
                    Optional<String> userAgent) {
        return getLockService(namespace, userAgent).currentTimeMillis();
    }

    @POST
    @Path("lock/log-current-state")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Idempotent
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/log-current-state")
    public void logCurrentState(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace,
            @Safe
                    @HeaderParam(TimelockNamespaces.USER_AGENT_HEADER)
                    @Handle.Header(TimelockNamespaces.USER_AGENT_HEADER)
                    Optional<String> userAgent) {
        getLockService(namespace, userAgent).logCurrentState();
    }

    // Remote lock service
    @POST
    @Path("lock/lock/{client}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Nullable
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/lock/{client}")
    @SuppressWarnings("NullableOptional") // null and empty are same over the wire
    public Optional<LockRefreshToken> lock(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace,
            @Safe @PathParam("client") @Handle.PathParam String client,
            @Handle.Body LockRequest request,
            @Safe
                    @HeaderParam(TimelockNamespaces.USER_AGENT_HEADER)
                    @Handle.Header(TimelockNamespaces.USER_AGENT_HEADER)
                    Optional<String> userAgent)
            throws InterruptedException {
        return Optional.ofNullable(getLockService(namespace, userAgent).lock(client, request));
    }

    @POST
    @Path("lock/lock")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Nullable
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/lock")
    @SuppressWarnings("NullableOptional") // null and empty are same over the wire
    public Optional<LockRefreshToken> lock(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace,
            @Handle.Body LockRequest request,
            @Safe
                    @HeaderParam(TimelockNamespaces.USER_AGENT_HEADER)
                    @Handle.Header(TimelockNamespaces.USER_AGENT_HEADER)
                    Optional<String> userAgent)
            throws InterruptedException {
        return lock(namespace, LockClient.ANONYMOUS.getClientId(), request, userAgent);
    }

    @POST
    @Path("lock/try-lock/{client}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/try-lock/{client}")
    public Optional<HeldLocksToken> lockAndGetHeldLocks(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace,
            @Safe @PathParam("client") @Handle.PathParam String client,
            @Handle.Body LockRequest request,
            @Safe
                    @HeaderParam(TimelockNamespaces.USER_AGENT_HEADER)
                    @Handle.Header(TimelockNamespaces.USER_AGENT_HEADER)
                    Optional<String> userAgent)
            throws InterruptedException {
        return Optional.ofNullable(getLockService(namespace, userAgent).lockAndGetHeldLocks(client, request));
    }

    @POST
    @Path("lock/try-lock")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/try-lock")
    public Optional<HeldLocksToken> lockAndGetHeldLocks(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace,
            @Handle.Body LockRequest request,
            @Safe
                    @HeaderParam(TimelockNamespaces.USER_AGENT_HEADER)
                    @Handle.Header(TimelockNamespaces.USER_AGENT_HEADER)
                    Optional<String> userAgent)
            throws InterruptedException {
        return lockAndGetHeldLocks(namespace, LockClient.ANONYMOUS.getClientId(), request, userAgent);
    }

    @POST
    @Path("lock/unlock")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @NonIdempotent
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/unlock")
    public boolean unlock(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace,
            @Handle.Body LockRefreshToken token,
            @Safe
                    @HeaderParam(TimelockNamespaces.USER_AGENT_HEADER)
                    @Handle.Header(TimelockNamespaces.USER_AGENT_HEADER)
                    Optional<String> userAgent) {
        return getLockService(namespace, userAgent).unlock(token);
    }

    @POST
    @Path("lock/refresh-lock-tokens")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Idempotent
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/refresh-lock-tokens")
    public Set<LockRefreshToken> refreshLockRefreshTokens(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace,
            @Handle.Body Iterable<LockRefreshToken> tokens,
            @Safe
                    @HeaderParam(TimelockNamespaces.USER_AGENT_HEADER)
                    @Handle.Header(TimelockNamespaces.USER_AGENT_HEADER)
                    Optional<String> userAgent) {
        return getLockService(namespace, userAgent).refreshLockRefreshTokens(tokens);
    }

    @POST
    @Path("lock/min-locked-in-version/{client}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Idempotent
    @Nullable
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/min-locked-in-version/{client}")
    @SuppressWarnings("NullableOptional") // null and empty are same over the wire
    public Optional<Long> getMinLockedInVersionId(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace,
            @Safe @PathParam("client") @Handle.PathParam String client,
            @Safe
                    @HeaderParam(TimelockNamespaces.USER_AGENT_HEADER)
                    @Handle.Header(TimelockNamespaces.USER_AGENT_HEADER)
                    Optional<String> userAgent) {
        return Optional.ofNullable(getLockService(namespace, userAgent).getMinLockedInVersionId(client));
    }

    @POST
    @Path("lock/min-locked-in-version")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Idempotent
    @Nullable
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/min-locked-in-version")
    @SuppressWarnings("NullableOptional") // null and empty are same over the wire
    public Optional<Long> getMinLockedInVersionIdForAnonymousClientString(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace,
            @Safe
                    @HeaderParam(TimelockNamespaces.USER_AGENT_HEADER)
                    @Handle.Header(TimelockNamespaces.USER_AGENT_HEADER)
                    Optional<String> userAgent) {
        return getMinLockedInVersionId(namespace, LockClient.ANONYMOUS.getClientId(), userAgent);
    }

    @POST
    @Path("lock/get-debugging-lock-state")
    @Idempotent
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Handle(method = HttpMethod.POST, path = "/{namespace}/lock/get-debugging-lock-state")
    public LockState getLockState(
            @Safe @PathParam("namespace") @Handle.PathParam String namespace,
            @Handle.Body LockDescriptor lock,
            @Safe
                    @HeaderParam(TimelockNamespaces.USER_AGENT_HEADER)
                    @Handle.Header(TimelockNamespaces.USER_AGENT_HEADER)
                    Optional<String> userAgent) {
        return getLockService(namespace, userAgent).getLockState(lock);
    }

    private LockService getLockService(String namespace, Optional<String> userAgent) {
        return namespaces.get(namespace, userAgent).getLockService();
    }
}
