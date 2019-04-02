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

package com.palantir.timelock.paxos;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.palantir.atlasdb.timelock.paxos.PaxosTimeLockConstants;
import com.palantir.paxos.BooleanPaxosResponse;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosPromise;
import com.palantir.paxos.PaxosProposal;
import com.palantir.paxos.PaxosProposalId;

/**
 * This interface is used internally to allow creating a single feign proxy where different clients can be injected
 * using {@link ClientAwarePaxosAcceptorAdapter} to create {@link PaxosAcceptor}s instead of creating a proxy per
 * client. This reduces the resource allocation when a new timelock node becomes the leader.
 */
@Path("/" + PaxosTimeLockConstants.INTERNAL_NAMESPACE
        + "/" + PaxosTimeLockConstants.CLIENT_PAXOS_NAMESPACE
        + "/{client}"
        + "/acceptor")
interface ClientAwarePaxosAcceptor {
    @POST
    @Path("prepare/{seq}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    PaxosPromise prepare(@PathParam("client") String client, @PathParam("seq") long seq, PaxosProposalId pid);


    @POST
    @Path("accept/{seq}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    BooleanPaxosResponse accept(@PathParam("client") String client, @PathParam("seq") long seq, PaxosProposal proposal);


    @POST
    @Path("latest-sequence-prepared-or-accepted")
    @Produces(MediaType.APPLICATION_JSON)
    long getLatestSequencePreparedOrAccepted(@PathParam("client") String client);
}
