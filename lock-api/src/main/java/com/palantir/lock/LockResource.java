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
 * See {@link LockService} and {@link LockRpcClient}.
 */
@Path("/lock")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class LockResource {
    private final LockService lockService;

    public LockResource(LockService lockService) {
        this.lockService = lockService;
    }

    @POST
    @Path("lock-with-full-response/{client: .*}")
    @CancelableServerCall
    public Optional<LockResponse> lockWithFullLockResponse(@Safe @PathParam("client") LockClient client,
            LockRequest request) throws InterruptedException {
        return Optional.ofNullable(lockService.lockWithFullLockResponse(client, request));
    }

    @POST
    @Path("unlock-deprecated")
    public boolean unlock(HeldLocksToken token) {
        return lockService.unlock(token);
    }

    @POST
    @Path("unlock")
    public boolean unlock(LockRefreshToken token) {
        return lockService.unlock(token);
    }

    @POST
    @Path("unlock-simple")
    public boolean unlockSimple(SimpleHeldLocksToken token) {
        return lockService.unlockSimple(token);
    }

    @POST
    @Path("unlock-and-freeze")
    public boolean unlockAndFreeze(HeldLocksToken token) {
        return lockService.unlockAndFreeze(token);
    }

    @POST
    @Path("get-tokens/{client: .*}")
    public Set<HeldLocksToken> getTokens(@Safe @PathParam("client") LockClient client) {
        return lockService.getTokens(client);
    }

    @POST
    @Path("refresh-tokens")
    public Set<HeldLocksToken> refreshTokens(Iterable<HeldLocksToken> tokens) {
        return lockService.refreshTokens(tokens);
    }

    @POST
    @Path("refresh-grant")
    public Optional<HeldLocksGrant> refreshGrant(HeldLocksGrant grant) {
        return Optional.ofNullable(lockService.refreshGrant(grant));
    }

    @POST
    @Path("refresh-grant-id")
    public Optional<HeldLocksGrant> refreshGrant(BigInteger grantId) {
        return Optional.ofNullable(lockService.refreshGrant(grantId));
    }

    @POST
    @Path("convert-to-grant")
    public HeldLocksGrant convertToGrant(HeldLocksToken token) {
        return lockService.convertToGrant(token);
    }

    @POST
    @Path("use-grant/{client: .*}")
    public HeldLocksToken useGrant(@Safe @PathParam("client") LockClient client, HeldLocksGrant grant) {
        return lockService.useGrant(client, grant);
    }

    @POST
    @Path("use-grant-id/{client: .*}")
    public HeldLocksToken useGrant(@Safe @PathParam("client") LockClient client, BigInteger grantId) {
        return lockService.useGrant(client, grantId);
    }

    @POST
    @Path("min-locked-in-version-id")
    public Optional<Long> getMinLockedInVersionId() {
        return Optional.ofNullable(lockService.getMinLockedInVersionId());
    }

    @POST
    @Path("min-locked-in-version-id-for-client/{client: .*}")
    public Optional<Long> getMinLockedInVersionId(@Safe @PathParam("client") LockClient client) {
        return Optional.ofNullable(lockService.getMinLockedInVersionId(client));
    }

    @POST
    @Path("min-locked-in-version/{client: .*}")
    public Optional<Long> getMinLockedInVersionId(@Safe @PathParam("client") String client) {
        return Optional.ofNullable(lockService.getMinLockedInVersionId(client));
    }

    @POST
    @Path("lock-server-options")
    public LockServerOptions getLockServerOptions() {
        return lockService.getLockServerOptions();
    }

    @POST
    @Path("lock/{client: .*}")
    public Optional<LockRefreshToken> lock(@Safe @PathParam("client") String client, LockRequest request)
            throws InterruptedException {
        return Optional.ofNullable(lockService.lock(client, request));
    }

    @POST
    @Path("try-lock/{client: .*}")
    public Optional<HeldLocksToken> lockAndGetHeldLocks(@Safe @PathParam("client") String client, LockRequest request)
            throws InterruptedException {
        return Optional.ofNullable(lockService.lockAndGetHeldLocks(client, request));
    }

    @POST
    @Path("refresh-lock-tokens")
    public Set<LockRefreshToken> refreshLockRefreshTokens(Iterable<LockRefreshToken> tokens) {
        return lockService.refreshLockRefreshTokens(tokens);
    }

    @POST
    @Path("current-time-millis")
    public long currentTimeMillis() {
        return lockService.currentTimeMillis();
    }

    @POST
    @Path("log-current-state")
    public void logCurrentState() {
        lockService.logCurrentState();
    }
}
