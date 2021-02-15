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

package com.palantir.atlasdb.timelock.lock.v1;

import com.palantir.lock.ConjureLockRefreshToken;
import com.palantir.lock.ConjureLockV1Request;
import com.palantir.lock.ConjureSimpleHeldLocksToken;
import com.palantir.lock.HeldLocksToken;
import com.palantir.tokens.auth.AuthHeader;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.ws.rs.Consumes;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/*
 * This interface exists because the semantics of JAX-RS matching involve selecting a resource AND THEN a path.
 * The technique of creating adapters from the Conjure-generated interfaces thus works for at most one resource.
 */
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Path("/lk/")
public interface ConjureLockV1ShimService {
    @POST
    @Path("laghl/{namespace}")
    Optional<HeldLocksToken> lockAndGetHeldLocks(
            @HeaderParam("Authorization") AuthHeader authHeader,
            @PathParam("namespace") String namespace,
            ConjureLockV1Request request);

    @POST
    @Path("rlrt/{namespace}")
    Set<ConjureLockRefreshToken> refreshLockRefreshTokens(
            @HeaderParam("Authorization") AuthHeader authHeader,
            @PathParam("namespace") String namespace,
            List<ConjureLockRefreshToken> request);

    @POST
    @Path("us/{namespace}")
    boolean unlockSimple(
            @HeaderParam("Authorization") AuthHeader authHeader,
            @PathParam("namespace") String namespace,
            ConjureSimpleHeldLocksToken request);
}
