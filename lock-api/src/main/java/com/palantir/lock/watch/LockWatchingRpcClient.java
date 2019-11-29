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

package com.palantir.lock.watch;

import java.util.OptionalLong;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/{namespace}/lock-watch")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public interface LockWatchingRpcClient {
    @POST
    @Path("start-watching")
    void startWatching(@PathParam("namespace") String namespace, LockWatchRequest lockWatchRequest);

    // as usual, GET is allowed to be cached, and we are not allowed to use a stale result
    @POST
    @Path("watch-state")
    LockWatchStateUpdate getWatchStateUpdate(@PathParam("namespace") String namespace, OptionalLong lastKnownVersion);
}
