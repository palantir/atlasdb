/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

import com.palantir.paxos.Client;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosLearner;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

@Path("/" + PaxosTimeLockConstants.INTERNAL_NAMESPACE
        + "/" + PaxosTimeLockConstants.CLIENT_PAXOS_NAMESPACE
        + "/{client: [a-zA-Z0-9_-]+}")
public final class TimestampPaxosResource {
    private final LocalPaxosComponents paxosComponents;

    TimestampPaxosResource(LocalPaxosComponents paxosComponents) {
        this.paxosComponents = paxosComponents;
    }

    @Path("/learner")
    public PaxosLearner getPaxosLearner(@PathParam("client") String client) {
        return paxosComponents.learner(Client.of(client));
    }

    @Path("/acceptor")
    public PaxosAcceptor getPaxosAcceptor(@PathParam("client") String client) {
        return paxosComponents.acceptor(Client.of(client));
    }

}
