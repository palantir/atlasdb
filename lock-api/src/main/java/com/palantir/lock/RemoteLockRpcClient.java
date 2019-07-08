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

import java.util.Optional;
import java.util.Set;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.palantir.logsafe.Safe;

/**
 * See {@link RemoteLockService}.
 */
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public interface RemoteLockRpcClient {
    @POST
    @Path("lock/{client: .*}")
    Optional<LockRefreshToken> lock(@Safe @PathParam("client") String client, LockRequest request)
            throws InterruptedException;

    @POST
    @Path("try-lock/{client: .*}")
    Optional<HeldLocksToken> lockAndGetHeldLocks(@Safe @PathParam("client") String client, LockRequest request)
            throws InterruptedException;

    @POST
    @Path("unlock")
    boolean unlock(LockRefreshToken token);

    @POST
    @Path("refresh-lock-tokens")
    Set<LockRefreshToken> refreshLockRefreshTokens(Iterable<LockRefreshToken> tokens);

    @POST
    @Path("min-locked-in-version/{client: .*}")
    Optional<Long> getMinLockedInVersionId(@Safe @PathParam("client") String client);

    @POST
    @Path("current-time-millis")
    long currentTimeMillis();

    @POST
    @Path("log-current-state")
    void logCurrentState();
}
