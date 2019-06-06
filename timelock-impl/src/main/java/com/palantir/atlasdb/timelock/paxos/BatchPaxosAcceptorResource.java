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

import static java.util.stream.Collectors.toSet;

import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.SetMultimap;
import com.palantir.common.streams.KeyedStream;
import com.palantir.conjure.java.api.errors.ErrorType;
import com.palantir.conjure.java.api.errors.ServiceException;
import com.palantir.logsafe.SafeArg;
import com.palantir.paxos.BooleanPaxosResponse;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosPromise;
import com.palantir.paxos.PaxosProposal;
import com.palantir.paxos.PaxosProposalId;

public class BatchPaxosAcceptorResource implements BatchPaxosAcceptor {

    private static final Logger log = LoggerFactory.getLogger(BatchPaxosAcceptorResource.class);

    @VisibleForTesting
    static final ErrorType CACHE_KEY_NOT_FOUND =
            ErrorType.create(ErrorType.Code.NOT_FOUND, "TimelockBatchPaxosAcceptor:CacheKeyNotFound");

    private final AcceptorCache acceptorCache;
    private final PaxosComponents paxosComponents;

    public BatchPaxosAcceptorResource(PaxosComponents paxosComponents, AcceptorCache acceptorCache) {
        this.paxosComponents = paxosComponents;
        this.acceptorCache = acceptorCache;
    }

    @Override
    public SetMultimap<Client, WithSeq<PaxosPromise>> prepare(
            SetMultimap<Client, WithSeq<PaxosProposalId>> promiseWithSeqRequestsByClient) {
        SetMultimap<Client, WithSeq<PaxosPromise>> results = KeyedStream.stream(
                promiseWithSeqRequestsByClient)
                .map((client, paxosProposalIdWithSeq) -> {
                    PaxosPromise promise = paxosComponents.acceptor(client)
                            .prepare(paxosProposalIdWithSeq.seq(), paxosProposalIdWithSeq.value());
                    return WithSeq.of(paxosProposalIdWithSeq.seq(), promise);
                })
                .collectToSetMultimap();
        primeCache(promiseWithSeqRequestsByClient.keySet());
        return results;
    }

    @Override
    public SetMultimap<Client, WithSeq<BooleanPaxosResponse>> accept(
            SetMultimap<Client, PaxosProposal> proposalRequestsByClient) {
        SetMultimap<Client, WithSeq<BooleanPaxosResponse>> results = KeyedStream.stream(proposalRequestsByClient)
                .map((client, paxosProposal) -> {
                    long seq = paxosProposal.getValue().getRound();
                    BooleanPaxosResponse ack = paxosComponents.acceptor(client)
                            .accept(seq, paxosProposal);
                    return WithSeq.of(seq, ack);
                })
                .collectToSetMultimap();
        primeCache(proposalRequestsByClient.keySet());
        return results;
    }

    @Override
    public AcceptorCacheDigest latestSequencesPreparedOrAccepted(
            Optional<AcceptorCacheKey> maybeCacheKey,
            Set<Client> clients) {
        primeCache(clients);
        if (!maybeCacheKey.isPresent()) {
            return acceptorCache.getAllUpdates();
        }

        AcceptorCacheKey cacheKey = maybeCacheKey.get();
        return latestSequencesPreparedOrAcceptedCached(cacheKey)
                .orElseGet(() -> emptyDigest(cacheKey));
    }

    @Override
    public Optional<AcceptorCacheDigest> latestSequencesPreparedOrAcceptedCached(AcceptorCacheKey cacheKey) {
        try {
            return acceptorCache.updatesSinceCacheKey(cacheKey);
        } catch (InvalidAcceptorCacheKeyException e) {
            log.info("Cache key is invalid", e);
            throw Errors.invalidCacheKeyException(cacheKey);
        }
    }

    private static AcceptorCacheDigest emptyDigest(AcceptorCacheKey cacheKey) {
        return ImmutableAcceptorCacheDigest.builder()
                .newCacheKey(cacheKey)
                .build();
    }

    private void primeCache(Set<Client> clients) {
        Set<WithSeq<Client>> latestSequences = KeyedStream.of(clients)
                .map(paxosComponents::acceptor)
                .map(PaxosAcceptor::getLatestSequencePreparedOrAccepted)
                .map((client, latestSeq) -> WithSeq.of(latestSeq, client))
                .values()
                .collect(toSet());

        acceptorCache.updateSequenceNumbers(latestSequences);
    }

    private static final class Errors {

        static ServiceException invalidCacheKeyException(AcceptorCacheKey cacheKey) {
            return new ServiceException(CACHE_KEY_NOT_FOUND, SafeArg.of("cacheKey", cacheKey));
        }
    }

}
