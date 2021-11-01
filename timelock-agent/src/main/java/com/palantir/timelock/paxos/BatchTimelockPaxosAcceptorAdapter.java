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

package com.palantir.timelock.paxos;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import com.palantir.atlasdb.timelock.paxos.BatchPaxosAcceptorRpcClient;
import com.palantir.atlasdb.timelock.paxos.PaxosUseCase;
import com.palantir.atlasdb.timelock.paxos.WithSeq;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.paxos.BooleanPaxosResponse;
import com.palantir.paxos.Client;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosPromise;
import com.palantir.paxos.PaxosProposal;
import com.palantir.paxos.PaxosProposalId;
import java.util.Set;

public class BatchTimelockPaxosAcceptorAdapter implements PaxosAcceptor {
    private final PaxosUseCase paxosUseCase;
    private final Client client;
    private final BatchPaxosAcceptorRpcClient rpcClient;

    public BatchTimelockPaxosAcceptorAdapter(
            PaxosUseCase paxosUseCase, Client client, BatchPaxosAcceptorRpcClient rpcClient) {
        this.paxosUseCase = paxosUseCase;
        this.client = client;
        this.rpcClient = rpcClient;
    }

    public static PaxosAcceptor singleLeader(BatchPaxosAcceptorRpcClient rpcClient) {
        return new BatchTimelockPaxosAcceptorAdapter(
                PaxosUseCase.LEADER_FOR_ALL_CLIENTS, PaxosUseCase.PSEUDO_LEADERSHIP_CLIENT, rpcClient);
    }

    @Override
    public PaxosPromise prepare(long seq, PaxosProposalId pid) {
        Set<WithSeq<PaxosPromise>> result =
                rpcClient.prepare(paxosUseCase, asMultimap(seq, pid)).get(client);
        checkResult(result);
        return Iterables.getOnlyElement(result).value();
    }

    @Override
    public BooleanPaxosResponse accept(long _seq, PaxosProposal proposal) {
        Set<WithSeq<BooleanPaxosResponse>> result = rpcClient
                .accept(paxosUseCase, ImmutableSetMultimap.of(client, proposal))
                .get(client);
        checkResult(result);
        return Iterables.getOnlyElement(result).value();
    }

    @Override
    public long getLatestSequencePreparedOrAccepted() {
        // TODO(gmaretic): this makes sense for this adapter since we only use a single client
        return rpcClient
                .latestSequencesPreparedOrAccepted(paxosUseCase, null, ImmutableSet.of(client))
                .updates()
                .get(client);
    }

    private <T> void checkResult(Set<T> result) {
        Preconditions.checkState(
                result.size() == 1,
                "Unexpected result {} in a call for client {}.",
                SafeArg.of("result", result),
                SafeArg.of("client", client));
    }

    private SetMultimap<Client, WithSeq<PaxosProposalId>> asMultimap(long seq, PaxosProposalId pid) {
        return ImmutableSetMultimap.of(client, WithSeq.of(pid, seq));
    }
}
