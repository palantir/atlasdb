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

package com.palantir.atlasdb.timelock.paxos;

import com.palantir.atlasdb.metrics.Timed;
import com.palantir.paxos.BooleanPaxosResponse;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosPromise;
import com.palantir.paxos.PaxosProposal;
import com.palantir.paxos.PaxosProposalId;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/**
 * Encapsulates a {@link PaxosAcceptor} but at a defined path for routing purposes, to avoid conflicts when attempting
 * to register multiple resources within the internal or leader paxos namespaces.
 */
@Path("/" + PaxosTimeLockConstants.INTERNAL_NAMESPACE
        + "/" + PaxosTimeLockConstants.LEADER_PAXOS_NAMESPACE
        + "/acceptor")
public class LeaderAcceptorResource {
    private final PaxosAcceptor acceptor;

    public LeaderAcceptorResource(PaxosAcceptor acceptor) {
        this.acceptor = acceptor;
    }

    @POST
    @Path("prepare/{seq}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Timed
    public PaxosPromise prepare(@PathParam("seq") long seq, PaxosProposalId pid) {
        return acceptor.prepare(seq, pid);
    }

    @POST
    @Path("accept/{seq}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public BooleanPaxosResponse accept(@PathParam("seq") long seq, PaxosProposal proposal) {
        return acceptor.accept(seq, proposal);
    }

    @POST // This is marked as a POST because we cannot accept stale or cached results for this method.
    @Path("latest-sequence-prepared-or-accepted")
    @Produces(MediaType.APPLICATION_JSON)
    public long getLatestSequencePreparedOrAccepted() {
        return acceptor.getLatestSequencePreparedOrAccepted();
    }
}
