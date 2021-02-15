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
package com.palantir.paxos;

import com.palantir.atlasdb.metrics.Timed;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/acceptor")
public interface PaxosAcceptor {
    long NO_LOG_ENTRY = -1L;

    /**
     * The acceptor prepares for a given proposal by either promising not to accept future proposals
     * or rejecting the proposal.
     *
     * @param seq the number identifying this instance of paxos
     * @param pid the proposal to prepare for
     * @return a paxos promise not to accept lower numbered proposals
     */
    @POST
    @Path("prepare/{seq}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Timed
    PaxosPromise prepare(@PathParam("seq") long seq, PaxosProposalId pid);

    /**
     * The acceptor decides whether to accept or reject a given proposal.
     *
     * @param seq the number identifying this instance of paxos
     * @param proposal the proposal in question
     * @return a paxos message indicating if the proposal was accepted or rejected
     */
    @POST
    @Path("accept/{seq}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Timed
    BooleanPaxosResponse accept(@PathParam("seq") long seq, PaxosProposal proposal);

    /**
     * Gets the sequence number of the acceptor's most recent known round.
     *
     * @return the sequence number of the most recent round or {@value NO_LOG_ENTRY} if this
     *         acceptor has not prepared or accepted any rounds
     */
    @POST // This is marked as a POST because we cannot accept stale or cached results for this method.
    @Path("latest-sequence-prepared-or-accepted")
    @Produces(MediaType.APPLICATION_JSON)
    @Timed
    long getLatestSequencePreparedOrAccepted();
}
