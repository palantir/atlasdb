/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.timelock.paxos;

import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.google.common.collect.Maps;
import com.palantir.leader.PingableLeader;

@Path("/{client: [a-zA-Z0-9_-]+}/leader")
public final class PingableLeaderLocator {
    private final Map<String, PingableLeader> leaders;

    public PingableLeaderLocator() {
        this.leaders = Maps.newConcurrentMap();
    }

    public void register(String client, PingableLeader pingableLeader) {
        leaders.putIfAbsent(client, pingableLeader);
    }

    public boolean hasClient(String client) {
        return leaders.containsKey(client);
    }

    @Path("ping")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public boolean ping(@PathParam("client") String client) {
        PingableLeader leader = getLeaderOrThrow(client);
        return leader.ping();
    }

    @Path("uuid")
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String getUUID(@PathParam("client") String client) {
        PingableLeader leader = getLeaderOrThrow(client);
        return leader.getUUID();
    }

    private PingableLeader getLeaderOrThrow(@PathParam("client") String client) {
        PingableLeader leader = leaders.get(client);
        if (leader == null) {
            throw new NotFoundException("Client " + client + " not registered.");
        }
        return leader;
    }
}
