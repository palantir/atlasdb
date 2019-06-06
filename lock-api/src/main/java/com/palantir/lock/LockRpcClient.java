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

import java.math.BigInteger;
import java.util.Optional;
import java.util.Set;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.palantir.annotations.remoting.CancelableServerCall;
import com.palantir.logsafe.Safe;

/**
 * See {@link LockService}.
 */
@Path("/lock")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public interface LockRpcClient extends RemoteLockRpcClient {
    @POST
    @Path("lock-with-full-response/{client: .*}")
    @CancelableServerCall
    Optional<LockResponse> lockWithFullLockResponse(@Safe @PathParam("client") LockClient client, LockRequest request)
            throws InterruptedException;

    @POST
    @Path("unlock-deprecated")
    @Deprecated
    boolean unlock(HeldLocksToken token);

    @POST
    @Path("unlock-simple")
    boolean unlockSimple(SimpleHeldLocksToken token);

    @POST
    @Path("unlock-and-freeze")
    boolean unlockAndFreeze(HeldLocksToken token);

    @POST
    @Path("get-tokens/{client: .*}")
    Set<HeldLocksToken> getTokens(@Safe @PathParam("client") LockClient client);

    @POST
    @Path("refresh-tokens")
    @Deprecated
    Set<HeldLocksToken> refreshTokens(Iterable<HeldLocksToken> tokens);

    @POST
    @Path("refresh-grant")
    Optional<HeldLocksGrant> refreshGrant(HeldLocksGrant grant);

    @POST
    @Path("refresh-grant-id")
    Optional<HeldLocksGrant> refreshGrant(BigInteger grantId);

    @POST
    @Path("convert-to-grant")
    HeldLocksGrant convertToGrant(HeldLocksToken token);

    @POST
    @Path("use-grant/{client: .*}")
    HeldLocksToken useGrant(@Safe @PathParam("client") LockClient client, HeldLocksGrant grant);

    @POST
    @Path("use-grant-id/{client: .*}")
    HeldLocksToken useGrant(@Safe @PathParam("client") LockClient client, BigInteger grantId);

    @POST
    @Path("min-locked-in-version-id")
    @Deprecated
    Optional<Long> getMinLockedInVersionId();

    @POST
    @Path("min-locked-in-version-id-for-client/{client: .*}")
    Optional<Long> getMinLockedInVersionId(@Safe @PathParam("client") LockClient client);

    @POST
    @Path("lock-server-options")
    LockServerOptions getLockServerOptions();
}
