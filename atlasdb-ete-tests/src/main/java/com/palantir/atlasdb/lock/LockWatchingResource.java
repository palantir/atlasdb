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

package com.palantir.atlasdb.lock;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.palantir.atlasdb.lockwatch.LockWatchState;
import com.palantir.lock.v2.LockToken;

@Path("lock-watch")
public interface LockWatchingResource {
    @POST
    @Path("register-watch")
    void register(@QueryParam("rowName") String rowName);

    @POST
    @Path("deregister-watch")
    void deregister(@QueryParam("rowName") String rowName);

    @POST
    @Path("timelock-lock")
    @Produces(MediaType.APPLICATION_JSON)
    LockToken lock(@QueryParam("rowName") String rowName);

    @POST
    @Path("timelock-unlock")
    @Consumes(MediaType.APPLICATION_JSON)
    void unlock(LockToken token);

    @POST
    @Path("transaction")
    void transaction(@QueryParam("rowName") String rowName);

    @POST
    @Path("get-watches")
    @Produces(MediaType.APPLICATION_JSON)
    LockWatchState getWatches();
}
