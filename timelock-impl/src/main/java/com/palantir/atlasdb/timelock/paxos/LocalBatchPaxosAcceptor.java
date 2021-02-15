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

import com.google.common.collect.SetMultimap;
import com.palantir.common.streams.KeyedStream;
import com.palantir.paxos.BooleanPaxosResponse;
import com.palantir.paxos.Client;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosPromise;
import com.palantir.paxos.PaxosProposal;
import com.palantir.paxos.PaxosProposalId;
import java.util.Optional;
import java.util.Set;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.HttpHeaders;

public class LocalBatchPaxosAcceptor implements BatchPaxosAcceptor {

    private final AcceptorCache acceptorCache;
    private final LocalPaxosComponents paxosComponents;

    LocalBatchPaxosAcceptor(LocalPaxosComponents paxosComponents, AcceptorCache acceptorCache) {
        this.paxosComponents = paxosComponents;
        this.acceptorCache = acceptorCache;
    }

    @Override
    public SetMultimap<Client, WithSeq<PaxosPromise>> prepare(
            SetMultimap<Client, WithSeq<PaxosProposalId>> promiseWithSeqRequestsByClient) {
        SetMultimap<Client, WithSeq<PaxosPromise>> results = KeyedStream.stream(promiseWithSeqRequestsByClient)
                .map((client, paxosProposalIdWithSeq) -> {
                    PaxosPromise promise = paxosComponents
                            .acceptor(client)
                            .prepare(paxosProposalIdWithSeq.seq(), paxosProposalIdWithSeq.value());
                    return paxosProposalIdWithSeq.withNewValue(promise);
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
                    BooleanPaxosResponse ack = paxosComponents.acceptor(client).accept(seq, paxosProposal);
                    return WithSeq.of(ack, seq);
                })
                .collectToSetMultimap();
        primeCache(proposalRequestsByClient.keySet());
        return results;
    }

    @Override
    public AcceptorCacheDigest latestSequencesPreparedOrAccepted(
            @QueryParam(HttpHeaders.IF_MATCH) Optional<AcceptorCacheKey> maybeCacheKey, Set<Client> clients)
            throws InvalidAcceptorCacheKeyException {
        primeCache(clients);
        if (!maybeCacheKey.isPresent()) {
            return acceptorCache.getAllUpdates();
        }

        AcceptorCacheKey cacheKey = maybeCacheKey.get();
        return latestSequencesPreparedOrAcceptedCached(cacheKey).orElseGet(() -> emptyDigest(cacheKey));
    }

    @Override
    public Optional<AcceptorCacheDigest> latestSequencesPreparedOrAcceptedCached(AcceptorCacheKey cacheKey)
            throws InvalidAcceptorCacheKeyException {
        return acceptorCache.updatesSinceCacheKey(cacheKey);
    }

    private static AcceptorCacheDigest emptyDigest(AcceptorCacheKey cacheKey) {
        return ImmutableAcceptorCacheDigest.builder()
                .newCacheKey(cacheKey)
                // HACK: this is okay, the digest is empty so doesn't do anything, Long.MIN_VALUE means it will never
                // accepted as the latest cache key
                .cacheTimestamp(Long.MIN_VALUE)
                .build();
    }

    private void primeCache(Set<Client> clients) {
        Set<WithSeq<Client>> latestSequences = KeyedStream.of(clients)
                .map(paxosComponents::acceptor)
                .map(PaxosAcceptor::getLatestSequencePreparedOrAccepted)
                .map(WithSeq::of)
                .values()
                .collect(toSet());

        acceptorCache.updateSequenceNumbers(latestSequences);
    }
}
