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

import com.google.common.collect.SetMultimap;
import com.palantir.conjure.java.api.errors.ErrorType;
import com.palantir.paxos.BooleanPaxosResponse;
import com.palantir.paxos.Client;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosPromise;
import com.palantir.paxos.PaxosProposal;
import com.palantir.paxos.PaxosProposalId;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

@Path("/" + PaxosTimeLockConstants.INTERNAL_NAMESPACE
        + "/{useCase}"
        + "/" + PaxosTimeLockConstants.BATCH_INTERNAL_NAMESPACE
        + "/acceptor")
public interface BatchPaxosAcceptorRpcClient {

    ErrorType CACHE_KEY_NOT_FOUND =
            ErrorType.create(ErrorType.Code.NOT_FOUND, "TimelockBatchPaxosAcceptor:CacheKeyNotFound");

    /**
     * Batch counterpart to {@link PaxosAcceptor#prepare}. For a given {@link Client} on paxos instance {@code seq},
     * the acceptor prepares for a given proposal ({@link PaxosProposalId}) by either promising not to accept future
     * proposals or rejecting the proposal.
     * <p>
     * @param paxosUseCase whether this is a timestamp paxos or leader paxos batch call
     * @param promiseWithSeqRequestsByClient for each {@link Client}, the set of paxos instances with the
     * {@link PaxosProposalId} to prepare for; {@link PaxosProposalId} is the id to prepare for
     * @return for each {@link Client} and each {@code seq}, a promise not to accept lower numbered proposals
     */
    @POST
    @Path("prepare")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    SetMultimap<Client, WithSeq<PaxosPromise>> prepare(
            @PathParam("useCase") PaxosUseCase paxosUseCase,
            SetMultimap<Client, WithSeq<PaxosProposalId>> promiseWithSeqRequestsByClient);

    /**
     * Batch counterpart to {@link PaxosAcceptor#accept}. For a given {@link Client} on paxos instance {@code seq}, the
     * acceptor decides whether to accept or reject a given proposal ({@link PaxosProposal}).
     * <p>
     * @param paxosUseCase whether this is a timestamp paxos or leader paxos batch call
     * @param proposalRequestsByClientAndSeq for each {@link Client}, the set of paxos instances tied to a particular
     * {@link PaxosProposal} to respond to; {@link PaxosProposal} the proposal in question for the above {@link Client}
     * and {@code seq}
     * @return for each {@link Client} and each {@code seq}, a paxos message indicating if the proposal was accepted or
     * rejected
     */
    @POST
    @Path("accept")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    SetMultimap<Client, WithSeq<BooleanPaxosResponse>> accept(
            @PathParam("useCase") PaxosUseCase paxosUseCase,
            SetMultimap<Client, PaxosProposal> proposalRequestsByClientAndSeq);

    /**
     * Returns all latest sequences prepared or accepted for the provided {@link Client}s and the next cache key to use
     * with {@link BatchPaxosAcceptor#latestSequencesPreparedOrAcceptedCached}.
     * <p>
     * If a provided {@code cacheKey} is valid and not expired, it will force fetching latest sequence for the given
     * {@code clients} and include those sequences as well as any other updates past the {@code cacheKey}. If an
     * acceptor has received multiple proposals at multiple sequence numbers for the same client past {@code cacheKey},
     * it will return the sequence number for the latest proposal.
     * <p>
     * If a provided {@code cacheKey} has expired/is invalid, a {@code 404 Not Found} is returned with the above conjure
     * error {@link BatchPaxosAcceptorRpcClient#CACHE_KEY_NOT_FOUND}. This request should be retried without a
     * {@code cacheKey} and also with a full set of clients to ensure a correct response.
     * <p>
     * If an acceptor has received multiple proposals at multiple sequence numbers for a given client, only the latest
     * sequence number is returned for that client.
     *
     * @param paxosUseCase whether this is a timestamp paxos or leader paxos batch call
     * @param clients clients to force getting latest sequences for
     * @return digest containing next cacheKey and updates since provided {@code cacheKey}
     */
    @POST
    @Path("latest-sequences-prepared-or-accepted")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    AcceptorCacheDigest latestSequencesPreparedOrAccepted(
            @PathParam("useCase") PaxosUseCase paxosUseCase,
            @QueryParam(HttpHeaders.IF_MATCH) @Nullable AcceptorCacheKey cacheKey,
            Set<Client> clients);

    /**
     * Returns all unseen latest sequences prepared or accepted past the given {@code cacheKey}. That is, if for a
     * client, an acceptor has received multiple proposals at multiple sequence numbers past {@code cacheKey}, it will
     * return the sequence number for the latest proposal.
     * <p>
     * If the {@code cacheKey} provided is invalid (expired or never issued) a {@code 404 Not Found} is
     * returned with the above conjure error {@link BatchPaxosAcceptorRpcClient#CACHE_KEY_NOT_FOUND}. The caller should
     * call {@link BatchPaxosAcceptor#latestSequencesPreparedOrAccepted} to get the desired sequences.
     * <p>
     * If a valid {@code cacheKey} is provided, it will return all unseen sequences from when that {@code cacheKey} was
     * issued. If the server has not prepared or accepted any sequences past that point, it will return
     * {@code 204 No Content} which translates to {@link Optional#empty}.
     * <p>
     * In addition to the updates, a new {@code cacheKey} is provided to use on the next invocation of this method to
     * minimise on payload size as this method is on the hot path.
     *
     * @param paxosUseCase whether this is a timestamp paxos or leader paxos batch call
     * @return {@code 204 No Content} if there is no update, a digest containing updates plus a new cache key, or a
     * {@code 404 Not Found} if the cache key is not valid.
     */
    @POST
    @Path("latest-sequences-prepared-or-accepted/cached")
    @Produces(MediaType.APPLICATION_JSON)
    Optional<AcceptorCacheDigest> latestSequencesPreparedOrAcceptedCached(
            @PathParam("useCase") PaxosUseCase paxosUseCase,
            @QueryParam(HttpHeaders.IF_MATCH) AcceptorCacheKey cacheKey);
}
