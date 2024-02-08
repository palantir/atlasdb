/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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
import com.palantir.conjure.java.undertow.annotations.Handle;
import com.palantir.conjure.java.undertow.annotations.HttpMethod;
import com.palantir.paxos.BooleanPaxosResponse;
import com.palantir.paxos.Client;
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
 * Encapsulates a {@link PaxosAcceptor} but at a defined path for routing purposes.
 */
@Path("/" + PaxosTimeLockConstants.INTERNAL_NAMESPACE
        + "/" + PaxosTimeLockConstants.CLIENT_PAXOS_NAMESPACE
        + "/{client: [a-zA-Z0-9_-]+}"
        + "/acceptor")
public final class TimestampPaxosAcceptorResource {
    private final LocalPaxosComponents paxosComponents;

    public TimestampPaxosAcceptorResource(LocalPaxosComponents paxosComponents) {
        this.paxosComponents = paxosComponents;
    }

    @POST
    @Path("prepare/{seq}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Timed
    @Handle(
            method = HttpMethod.POST,
            path = "/" + PaxosTimeLockConstants.INTERNAL_NAMESPACE
                    + "/" + PaxosTimeLockConstants.CLIENT_PAXOS_NAMESPACE
                    + "/{client}/acceptor/prepare/{seq}")
    public PaxosPromise prepare(
            @PathParam("client") @Handle.PathParam String client,
            @PathParam("seq") @Handle.PathParam long seq,
            @Handle.Body PaxosProposalId pid) {
        return getAcceptor(client).prepare(seq, pid);
    }

    @POST
    @Path("accept/{seq}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Timed
    @Handle(
            method = HttpMethod.POST,
            path = "/" + PaxosTimeLockConstants.INTERNAL_NAMESPACE
                    + "/" + PaxosTimeLockConstants.CLIENT_PAXOS_NAMESPACE
                    + "/{client}/acceptor/accept/{seq}")
    public BooleanPaxosResponse accept(
            @PathParam("client") @Handle.PathParam String client,
            @PathParam("seq") @Handle.PathParam long seq,
            @Handle.Body PaxosProposal proposal) {
        return getAcceptor(client).accept(seq, proposal);
    }

    @POST // This is marked as a POST because we cannot accept stale or cached results for this method.
    @Path("latest-sequence-prepared-or-accepted")
    @Produces(MediaType.APPLICATION_JSON)
    @Timed
    @Handle(
            method = HttpMethod.POST,
            path = "/" + PaxosTimeLockConstants.INTERNAL_NAMESPACE
                    + "/" + PaxosTimeLockConstants.CLIENT_PAXOS_NAMESPACE
                    + "/{client}/acceptor/latest-sequence-prepared-or-accepted")
    public long getLatestSequencePreparedOrAccepted(@PathParam("client") @Handle.PathParam String client) {
        return getAcceptor(client).getLatestSequencePreparedOrAccepted();
    }

    private PaxosAcceptor getAcceptor(String client) {
        return paxosComponents.acceptor(Client.of(client));
    }
}
