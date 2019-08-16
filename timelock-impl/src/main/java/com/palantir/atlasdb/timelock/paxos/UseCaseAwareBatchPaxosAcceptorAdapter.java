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

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.SetMultimap;
import com.palantir.paxos.BooleanPaxosResponse;
import com.palantir.paxos.PaxosPromise;
import com.palantir.paxos.PaxosProposal;
import com.palantir.paxos.PaxosProposalId;

public class UseCaseAwareBatchPaxosAcceptorAdapter implements BatchPaxosAcceptor {

    private final PaxosUseCase useCase;
    private final BatchPaxosAcceptorRpcClient rpcClient;

    private UseCaseAwareBatchPaxosAcceptorAdapter(PaxosUseCase useCase, BatchPaxosAcceptorRpcClient rpcClient) {
        this.useCase = useCase;
        this.rpcClient = rpcClient;
    }

    @Override
    public SetMultimap<Client, WithSeq<PaxosPromise>> prepare(
            SetMultimap<Client, WithSeq<PaxosProposalId>> promiseWithSeqRequestsByClient) {
        return rpcClient.prepare(useCase, promiseWithSeqRequestsByClient);
    }

    @Override
    public SetMultimap<Client, WithSeq<BooleanPaxosResponse>> accept(
            SetMultimap<Client, PaxosProposal> proposalRequestsByClientAndSeq) {
        return rpcClient.accept(useCase, proposalRequestsByClientAndSeq);
    }

    @Override
    public AcceptorCacheDigest latestSequencesPreparedOrAccepted(
            Optional<AcceptorCacheKey> cacheKey,
            Set<Client> clients) {
        return rpcClient.latestSequencesPreparedOrAccepted(useCase, cacheKey.orElse(null), clients);
    }

    @Override
    public Optional<AcceptorCacheDigest> latestSequencesPreparedOrAcceptedCached(AcceptorCacheKey cacheKey) {
        return rpcClient.latestSequencesPreparedOrAcceptedCached(useCase, cacheKey);
    }

    public static List<BatchPaxosAcceptor> wrap(PaxosUseCase useCase, List<BatchPaxosAcceptorRpcClient> remotes) {
        return remotes.stream()
                .map(rpcClient -> new UseCaseAwareBatchPaxosAcceptorAdapter(useCase, rpcClient))
                .collect(Collectors.toList());
    }
}
