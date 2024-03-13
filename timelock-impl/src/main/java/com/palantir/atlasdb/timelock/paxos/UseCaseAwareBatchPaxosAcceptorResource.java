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

import com.google.common.collect.SetMultimap;
import com.palantir.conjure.java.undertow.annotations.Handle;
import com.palantir.conjure.java.undertow.annotations.HttpMethod;
import com.palantir.conjure.java.undertow.annotations.ParamDecoder;
import com.palantir.paxos.BooleanPaxosResponse;
import com.palantir.paxos.Client;
import com.palantir.paxos.PaxosPromise;
import com.palantir.paxos.PaxosProposal;
import com.palantir.paxos.PaxosProposalId;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

@Path("/" + PaxosTimeLockConstants.INTERNAL_NAMESPACE
        + "/{useCase}/"
        + PaxosTimeLockConstants.BATCH_INTERNAL_NAMESPACE
        + "/acceptor")
public final class UseCaseAwareBatchPaxosAcceptorResource {
    private final Function<PaxosUseCase, BatchPaxosAcceptorResource> delegate;

    public UseCaseAwareBatchPaxosAcceptorResource(Function<PaxosUseCase, BatchPaxosAcceptorResource> delegate) {
        this.delegate = delegate;
    }

    @POST
    @Path("prepare")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Handle(
            method = HttpMethod.POST,
            path = "/" + PaxosTimeLockConstants.INTERNAL_NAMESPACE + "/{useCase}/"
                    + PaxosTimeLockConstants.BATCH_INTERNAL_NAMESPACE + "/acceptor/prepare")
    public SetMultimap<Client, WithSeq<PaxosPromise>> prepare(
            @PathParam("useCase") @Handle.PathParam(decoder = PaxosUseCaseDecoder.class) PaxosUseCase useCase,
            @Handle.Body SetMultimap<Client, WithSeq<PaxosProposalId>> promiseWithSeqRequestsByClient) {
        return getBatchAcceptor(useCase).prepare(promiseWithSeqRequestsByClient);
    }

    @POST
    @Path("accept")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Handle(
            method = HttpMethod.POST,
            path = "/" + PaxosTimeLockConstants.INTERNAL_NAMESPACE + "/{useCase}/"
                    + PaxosTimeLockConstants.BATCH_INTERNAL_NAMESPACE + "/acceptor/accept")
    public SetMultimap<Client, WithSeq<BooleanPaxosResponse>> accept(
            @PathParam("useCase") @Handle.PathParam(decoder = PaxosUseCaseDecoder.class) PaxosUseCase useCase,
            @Handle.Body SetMultimap<Client, PaxosProposal> proposalRequestsByClient) {
        return getBatchAcceptor(useCase).accept(proposalRequestsByClient);
    }

    @POST
    @Path("latest-sequences-prepared-or-accepted")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Handle(
            method = HttpMethod.POST,
            path = "/" + PaxosTimeLockConstants.INTERNAL_NAMESPACE + "/{useCase}/"
                    + PaxosTimeLockConstants.BATCH_INTERNAL_NAMESPACE
                    + "/acceptor/latest-sequences-prepared-or-accepted")
    public AcceptorCacheDigest latestSequencesPreparedOrAccepted(
            @PathParam("useCase") @Handle.PathParam(decoder = PaxosUseCaseDecoder.class) PaxosUseCase useCase,
            @QueryParam(HttpHeaders.IF_MATCH) @Handle.QueryParam("If-Match") Optional<AcceptorCacheKey> maybeCacheKey,
            @Handle.Body Set<Client> clients) {
        return getBatchAcceptor(useCase).latestSequencesPreparedOrAccepted(maybeCacheKey, clients);
    }

    @POST
    @Path("latest-sequences-prepared-or-accepted/cached")
    @Produces(MediaType.APPLICATION_JSON)
    @Handle(
            method = HttpMethod.POST,
            path = "/" + PaxosTimeLockConstants.INTERNAL_NAMESPACE + "/{useCase}/"
                    + PaxosTimeLockConstants.BATCH_INTERNAL_NAMESPACE
                    + "/acceptor/latest-sequences-prepared-or-accepted/cached")
    public Optional<AcceptorCacheDigest> latestSequencesPreparedOrAcceptedCached(
            @PathParam("useCase") @Handle.PathParam(decoder = PaxosUseCaseDecoder.class) PaxosUseCase useCase,
            @QueryParam(HttpHeaders.IF_MATCH) @Handle.QueryParam("If-Match") Optional<AcceptorCacheKey> cacheKey) {
        return getBatchAcceptor(useCase).latestSequencesPreparedOrAcceptedCached(cacheKey);
    }

    private BatchPaxosAcceptorResource getBatchAcceptor(PaxosUseCase useCase) {
        return delegate.apply(useCase);
    }

    enum PaxosUseCaseDecoder implements ParamDecoder<PaxosUseCase> {
        INSTANCE;

        @Override
        public PaxosUseCase decode(String value) {
            return PaxosUseCase.fromString(value);
        }
    }
}
