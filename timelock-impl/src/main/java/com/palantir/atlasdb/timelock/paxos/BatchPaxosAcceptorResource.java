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

import static com.palantir.atlasdb.timelock.paxos.BatchPaxosAcceptorRpcClient.CACHE_KEY_NOT_FOUND;

import com.google.common.collect.SetMultimap;
import com.palantir.conjure.java.api.errors.ServiceException;
import com.palantir.logsafe.SafeArg;
import com.palantir.paxos.BooleanPaxosResponse;
import com.palantir.paxos.Client;
import com.palantir.paxos.PaxosPromise;
import com.palantir.paxos.PaxosProposal;
import com.palantir.paxos.PaxosProposalId;
import java.util.Optional;
import java.util.Set;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/" + PaxosTimeLockConstants.BATCH_INTERNAL_NAMESPACE
        + "/acceptor")
public class BatchPaxosAcceptorResource {

    private static final Logger log = LoggerFactory.getLogger(BatchPaxosAcceptorResource.class);

    private final BatchPaxosAcceptor batchPaxosAcceptor;

    BatchPaxosAcceptorResource(BatchPaxosAcceptor batchPaxosAcceptor) {
        this.batchPaxosAcceptor = batchPaxosAcceptor;
    }

    @POST
    @Path("prepare")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public SetMultimap<Client, WithSeq<PaxosPromise>> prepare(
            SetMultimap<Client, WithSeq<PaxosProposalId>> promiseWithSeqRequestsByClient) {
        return batchPaxosAcceptor.prepare(promiseWithSeqRequestsByClient);
    }

    @POST
    @Path("accept")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public SetMultimap<Client, WithSeq<BooleanPaxosResponse>> accept(
            SetMultimap<Client, PaxosProposal> proposalRequestsByClient) {
        return batchPaxosAcceptor.accept(proposalRequestsByClient);
    }

    @POST
    @Path("latest-sequences-prepared-or-accepted")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public AcceptorCacheDigest latestSequencesPreparedOrAccepted(
            @QueryParam(HttpHeaders.IF_MATCH) Optional<AcceptorCacheKey> maybeCacheKey,
            Set<Client> clients) {
        try {
            return batchPaxosAcceptor.latestSequencesPreparedOrAccepted(maybeCacheKey, clients);
        } catch (InvalidAcceptorCacheKeyException e) {
            log.info("Cache key is invalid", e);
            throw Errors.invalidCacheKeyException(e.cacheKey());
        }
    }

    @POST
    @Path("latest-sequences-prepared-or-accepted/cached")
    @Produces(MediaType.APPLICATION_JSON)
    public Optional<AcceptorCacheDigest> latestSequencesPreparedOrAcceptedCached(
            @QueryParam(HttpHeaders.IF_MATCH) Optional<AcceptorCacheKey> cacheKey) {
        if (!cacheKey.isPresent()) {
            throw Errors.invalidCacheKeyException(null);
        }
        try {
            return batchPaxosAcceptor.latestSequencesPreparedOrAcceptedCached(cacheKey.get());
        } catch (InvalidAcceptorCacheKeyException e) {
            log.info("Cache key is invalid", e);
            throw Errors.invalidCacheKeyException(e.cacheKey());
        }
    }

    private static final class Errors {
        static ServiceException invalidCacheKeyException(AcceptorCacheKey cacheKey) {
            return new ServiceException(CACHE_KEY_NOT_FOUND, SafeArg.of("cacheKey", cacheKey));
        }
    }

}
