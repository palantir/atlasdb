/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock;

import com.palantir.annotations.remoting.CancelableServerCall;
import com.palantir.logsafe.Safe;
import java.math.BigInteger;
import java.util.Optional;
import java.util.Set;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/{namespace}/lock")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public interface LockRpcClient {
    @POST
    @Path("lock-with-full-response/{client: .*}")
    @CancelableServerCall
    Optional<LockResponse> lockWithFullLockResponse(
            @Safe @PathParam("namespace") String namespace,
            @Safe @PathParam("client") LockClient client,
            LockRequest request)
            throws InterruptedException;

    @POST
    @Path("unlock-deprecated")
    boolean unlock(@Safe @PathParam("namespace") String namespace, HeldLocksToken token);

    @POST
    @Path("unlock")
    boolean unlock(@Safe @PathParam("namespace") String namespace, LockRefreshToken token);

    @POST
    @Path("unlock-simple")
    boolean unlockSimple(@Safe @PathParam("namespace") String namespace, SimpleHeldLocksToken token);

    @POST
    @Path("unlock-and-freeze")
    boolean unlockAndFreeze(@Safe @PathParam("namespace") String namespace, HeldLocksToken token);

    @POST
    @Path("get-tokens/{client: .*}")
    Set<HeldLocksToken> getTokens(
            @Safe @PathParam("namespace") String namespace, @Safe @PathParam("client") LockClient client);

    @POST
    @Path("refresh-tokens")
    Set<HeldLocksToken> refreshTokens(@Safe @PathParam("namespace") String namespace, Iterable<HeldLocksToken> tokens);

    @POST
    @Path("refresh-grant")
    Optional<HeldLocksGrant> refreshGrant(@Safe @PathParam("namespace") String namespace, HeldLocksGrant grant);

    @POST
    @Path("refresh-grant-id")
    Optional<HeldLocksGrant> refreshGrant(@Safe @PathParam("namespace") String namespace, BigInteger grantId);

    @POST
    @Path("convert-to-grant")
    HeldLocksGrant convertToGrant(@Safe @PathParam("namespace") String namespace, HeldLocksToken token);

    @POST
    @Path("use-grant/{client: .*}")
    HeldLocksToken useGrant(
            @Safe @PathParam("namespace") String namespace,
            @Safe @PathParam("client") LockClient client,
            HeldLocksGrant grant);

    @POST
    @Path("use-grant-id/{client: .*}")
    HeldLocksToken useGrant(
            @Safe @PathParam("namespace") String namespace,
            @Safe @PathParam("client") LockClient client,
            BigInteger grantId);

    @POST
    @Path("min-locked-in-version-id")
    Optional<Long> getMinLockedInVersionId(@Safe @PathParam("namespace") String namespace);

    @POST
    @Path("min-locked-in-version-id-for-client/{client: .*}")
    Optional<Long> getMinLockedInVersionId(
            @Safe @PathParam("namespace") String namespace, @Safe @PathParam("client") LockClient client);

    @POST
    @Path("min-locked-in-version/{client: .*}")
    Optional<Long> getMinLockedInVersionId(
            @Safe @PathParam("namespace") String namespace, @Safe @PathParam("client") String client);

    @POST
    @Path("lock-server-options")
    LockServerOptions getLockServerOptions(@Safe @PathParam("namespace") String namespace);

    @POST
    @Path("lock/{client: .*}")
    Optional<LockRefreshToken> lock(
            @Safe @PathParam("namespace") String namespace,
            @Safe @PathParam("client") String client,
            LockRequest request)
            throws InterruptedException;

    @POST
    @Path("try-lock/{client: .*}")
    Optional<HeldLocksToken> lockAndGetHeldLocks(
            @Safe @PathParam("namespace") String namespace,
            @Safe @PathParam("client") String client,
            LockRequest request)
            throws InterruptedException;

    @POST
    @Path("refresh-lock-tokens")
    Set<LockRefreshToken> refreshLockRefreshTokens(
            @Safe @PathParam("namespace") String namespace, Iterable<LockRefreshToken> tokens);

    @POST
    @Path("current-time-millis")
    long currentTimeMillis(@Safe @PathParam("namespace") String namespace);

    @POST
    @Path("log-current-state")
    void logCurrentState(@Safe @PathParam("namespace") String namespace);
}
