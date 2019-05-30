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

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.ws.rs.Consumes;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

import com.palantir.paxos.BooleanPaxosResponse;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosPromise;
import com.palantir.paxos.PaxosProposal;
import com.palantir.paxos.PaxosProposalId;

@Path("/batch/acceptor")
public interface BatchPaxosAcceptor {

    long NO_LOG_ENTRY = PaxosAcceptor.NO_LOG_ENTRY;

    /**
     * Batch counterpart to {@link PaxosAcceptor#prepare}. For a given {@link Client} on paxos instance {@code seq},
     * the acceptor prepares for a given proposal ({@link PaxosProposalId}) by either promising not to accept future
     * proposals or rejecting the proposal.
     * <p>
     * @param promiseRequestsByClientAndSeq {@link ClientAndSeq} identifies the client and the instance of paxos to
     * prepare for; {@link PaxosProposalId} - for the above {@link ClientAndSeq} - is the id to prepare for
     * @return for each {@link ClientAndSeq}, a promise not to accept lower numbered proposals
     */
    @POST
    @Path("prepare")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    Map<ClientAndSeq, PaxosPromise> prepare(Map<ClientAndSeq, PaxosProposalId> promiseRequestsByClientAndSeq);


    /**
     * Batch counterpart to {@link PaxosAcceptor#accept}. For a given {@link Client} on paxos instance {@code seq}, the
     * acceptor decides whether to accept or reject a given proposal ({@link PaxosProposal}.
     * <p>
     * @param proposalRequestsByClientAndSeq {@link ClientAndSeq} identifies the client and the instance of paxos to
     * respond to; {@link PaxosProposal} the proposal in question for the above {@link ClientAndSeq}
     * @return for each {@link ClientAndSeq}, a paxos message indicating if the proposal was accepted or rejected
     */
    @POST
    @Path("accept")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    Map<ClientAndSeq, BooleanPaxosResponse> accept(Map<ClientAndSeq, PaxosProposal> proposalRequestsByClientAndSeq);

    /**
     * Returns all latest sequences prepared or accepted for the provided {@link Client}s and the next cache key to use
     * with {@link BatchPaxosAcceptor#latestSequencesPreparedOrAcceptedCached}.
     * <p>
     * If a provided {@code cacheKey} is valid and not expired, it will force fetching latest sequence for the given
     * {@code clients} and include those sequences as well as any other updates past the {@code cacheKey}.
     * <p>
     * If a provided {@code cacheKey} has expired/invalid, a {@code 412 Precondition Failed} is thrown and this request
     * should be retried without a {@code cacheKey} and also with a full set of clients to ensure a correct response.
     *
     * @param clients clients to force getting latest sequences for
     * @return digest containing next cacheKey and updates since provided {@code cacheKey}
     */
    @POST
    @Path("latest-sequences-prepared-or-accepted")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    AcceptorCacheDigest latestSequencesPreparedOrAccepted(
            @HeaderParam(HttpHeaders.IF_MATCH) Optional<AcceptorCacheKey> cacheKey,
            Set<Client> clients);

    /**
     * Returns all unseen latest sequences prepared or accepted past the given {@code cacheKey}.
     * <p>
     * If the {@code cacheKey} provided is invalid (expired or never issued) a {@code 412 Precondition Failed} is
     * returned. The caller should call {@link BatchPaxosAcceptor#latestSequencesPreparedOrAccepted} to get the desired
     * sequences.
     * <p>
     * If a valid {@code cacheKey} is provided, it will return all unseen sequences from when that {@code cacheKey} was
     * issued. If the server has not prepared or accepted any sequences past that point, it will return
     * {@code 204 No Content}.
     * <p>
     * In addition to the updates, a new {@code cacheKey} is provided to use on the next invocation of this method to
     * minimise on payload size as this method is on the hot path.
     *
     * @param cacheKey
     * @return {@code 204 No Content} if there is no update, a digest containing
     */
    @POST
    @Path("latest-sequences-prepared-or-accepted/cached")
    @Produces(MediaType.APPLICATION_JSON)
    Optional<AcceptorCacheDigest> latestSequencesPreparedOrAcceptedCached(
            @HeaderParam(HttpHeaders.IF_MATCH) AcceptorCacheKey cacheKey);


}
