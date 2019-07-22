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

package com.palantir.atlasdb.timelock.watch;

import java.util.Set;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/lock-watch")
public interface LockWatchResource {
    /**
     * In a single call checks the state of open watches and registers new ones.
     */
    @POST
    @Path("/register")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    WatchStateResponse registerOrGetStates(WatchStateQuery query);

    /**
     * Unregisters watches.
     */
    @DELETE
    @Path("/unregister")
    @Consumes(MediaType.APPLICATION_JSON)
    Set<WatchIdentifier> unregisterWatch(Set<WatchIdentifier> identifiers);

    // Not intended for remote callers, at least not right now.
    LockEventProcessor getEventProcessor();
}
