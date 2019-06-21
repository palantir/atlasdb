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

import java.util.UUID;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/**
 * A service that allows users of a {@link com.palantir.lock.v2.TimelockService} to track what is happening with their
 * locks. Specifically, users can register a watch over some lock IDs. Whenever a watched lock is locked or unlocked,
 * an event is published to this service.
 */
@Path("/lock-watch")
public interface LockWatchService {
    /**
     * Registers a watch for the provided {@link LockPredicate}.
     * @param predicate indicates which locks should be watched by this watch
     * @return UUID of the watch, that can be used for queries to
     */
    @PUT
    @Path("watch")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    UUID registerWatch(LockPredicate predicate);

    /**
     * Unregisters the watch with the specified UUID.
     * @throws NotFoundException if we do not recognise the watch identifier the user provides
     */
    @DELETE
    @Path("watch/{id}")
    void unregisterWatch(@PathParam("id") UUID watchIdentifier) throws NotFoundException;

    /**
     * Gets the state a given watch is in.
     *
     * @param watchIdentifier watch identifier
     * @return true if and only if some lock guarded by this watch was locked
     * @throws NotFoundException if we do not recognise the watch identifier the user provides
     */
    @POST
    @Path("watch/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    LockWatchState getWatchState(@PathParam("id") UUID watchIdentifier) throws NotFoundException;
}
