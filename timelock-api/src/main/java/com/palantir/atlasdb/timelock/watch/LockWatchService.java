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

import java.util.Map;
import java.util.Set;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
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
     * @param predicates indicates which locks should be watched
     * @return Identifiers for watches
     */
    @PUT
    @Path("/register")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    Map<LockPredicate, RegisterWatchResponse> getOrRegisterWatches(Set<LockPredicate> predicates);

    /**
     * Unregisters watches.
     * @return watches that were unregistered
     */
    @DELETE
    @Path("/unregister")
    @Consumes(MediaType.APPLICATION_JSON)
    Set<WatchIdentifier> unregisterWatch(Set<WatchIdentifier> identifiers);

    /**
     * Gets the state specified watches are in.
     * Keys won't be included if we don't know of a watch with that id.
     *
     * @param identifiers watch identifiers
     * @return true if and only if some lock guarded by this watch was locked
     */
    @POST
    @Path("/status")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    Map<WatchIdentifier, WatchIndexState> getWatchStates(Set<WatchIdentifier> identifiers);
}
