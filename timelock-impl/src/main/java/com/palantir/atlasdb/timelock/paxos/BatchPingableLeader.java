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

package com.palantir.atlasdb.timelock.paxos;

import java.util.Set;
import java.util.UUID;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.palantir.leader.PingableLeader;

@Path("/batch/leader")
public interface BatchPingableLeader {

    /**
     * Batch counterpart to {@link PingableLeader#ping}. If this call returns, then the server is reachable.
     * <p>
     * For the given set of {@code clients}, the remote server returns the clients for which it thinks it is the leader
     * for. Clients that the remote server is not the leader for will be excluded from the results.
     *
     * @param clients set of clients to check remote servers leadership on
     * @return clients which the remote server believes to be the leader
     */
    @GET
    @Path("ping")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    Set<Client> ping(Set<Client> clients);

    /**
     * Re-exported version of {@link PingableLeader#getUUID}. Returns the remote servers unique leadership identifier.
     * This will be the same across all clients that the remote server it is currently aware of. It is therefore safe to
     * coalesce ping calls to the same remote server for different clients.
     *
     * @return the remote servers unique leadership identifier
     */
    @GET
    @Path("uuid")
    @Produces(MediaType.APPLICATION_JSON)
    UUID uuid();
}
