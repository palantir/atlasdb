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

import java.util.Optional;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.timelock.paxos.PaxosRemoteClients.AsyncIsLatestSequencePreparedOrAccepted;
import com.palantir.paxos.BooleanPaxosResponse;
import com.palantir.paxos.PaxosPromise;
import com.palantir.paxos.PaxosProposal;
import com.palantir.paxos.PaxosProposalId;

public class DelegatingBatchPaxosAcceptorRpcClient implements BatchPaxosAcceptorRpcClient {

    private final AsyncIsLatestSequencePreparedOrAccepted isLatestSequenceOverride;
    private final BatchPaxosAcceptorRpcClient originalDelegate;

    public DelegatingBatchPaxosAcceptorRpcClient(
            AsyncIsLatestSequencePreparedOrAccepted isLatestSequenceOverride,
            BatchPaxosAcceptorRpcClient originalDelegate) {
        this.originalDelegate = originalDelegate;
        this.isLatestSequenceOverride = isLatestSequenceOverride;
    }

    @Override
    public SetMultimap<Client, WithSeq<PaxosPromise>> prepare(
            PaxosUseCase paxosUseCase,
            SetMultimap<Client, WithSeq<PaxosProposalId>> promiseWithSeqRequestsByClient) {
        return originalDelegate.prepare(paxosUseCase, promiseWithSeqRequestsByClient);
    }

    @Override
    public SetMultimap<Client, WithSeq<BooleanPaxosResponse>> accept(
            PaxosUseCase paxosUseCase,
            SetMultimap<Client, PaxosProposal> proposalRequestsByClientAndSeq) {
        return originalDelegate.accept(paxosUseCase, proposalRequestsByClientAndSeq);
    }

    @Override
    public ListenableFuture<AcceptorCacheDigest> latestSequencesPreparedOrAcceptedAsync(
            PaxosUseCase paxosUseCase,
            @Nullable AcceptorCacheKey cacheKey,
            Set<Client> clients) {
        return isLatestSequenceOverride.latestSequencesPreparedOrAccepted(
                paxosUseCase,
                Optional.ofNullable(cacheKey),
                clients);
    }

    @Override
    public ListenableFuture<Optional<AcceptorCacheDigest>> latestSequencesPreparedOrAcceptedCachedAsync(
            PaxosUseCase paxosUseCase,
            AcceptorCacheKey cacheKey) {
        return isLatestSequenceOverride.latestSequencesPreparedOrAcceptedCached(paxosUseCase, cacheKey);
    }

    @Override
    public AcceptorCacheDigest latestSequencesPreparedOrAccepted(
            PaxosUseCase paxosUseCase,
            @Nullable AcceptorCacheKey cacheKey,
            Set<Client> clients) {
        return AtlasFutures.getUnchecked(latestSequencesPreparedOrAcceptedAsync(paxosUseCase, cacheKey, clients));
    }

    @Override
    public Optional<AcceptorCacheDigest> latestSequencesPreparedOrAcceptedCached(
            PaxosUseCase paxosUseCase,
            AcceptorCacheKey cacheKey) {
        return AtlasFutures.getUnchecked(latestSequencesPreparedOrAcceptedCachedAsync(paxosUseCase, cacheKey));
    }
}
