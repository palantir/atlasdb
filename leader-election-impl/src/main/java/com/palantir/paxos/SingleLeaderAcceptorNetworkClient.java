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

package com.palantir.paxos;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class SingleLeaderAcceptorNetworkClient implements PaxosAcceptorNetworkClient {

    private final ImmutableList<PaxosAcceptor> acceptors;
    private final int quorumSize;
    private final Map<PaxosAcceptor, ExecutorService> executors;
    private final boolean cancelRemainingCalls;

    public SingleLeaderAcceptorNetworkClient(
            List<PaxosAcceptor> acceptors,
            int quorumSize,
            Map<PaxosAcceptor, ExecutorService> executors,
            boolean cancelRemainingCalls) {
        this.acceptors = ImmutableList.copyOf(acceptors);
        this.quorumSize = quorumSize;
        this.executors = executors;
        this.cancelRemainingCalls = cancelRemainingCalls;
    }

    @Override
    public PaxosResponses<PaxosPromise> prepare(long seq, PaxosProposalId proposalId) {
        return PaxosQuorumChecker.collectQuorumResponses(
                acceptors,
                acceptor -> acceptor.prepare(seq, proposalId),
                quorumSize,
                executors,
                PaxosQuorumChecker.DEFAULT_REMOTE_REQUESTS_TIMEOUT,
                cancelRemainingCalls).withoutRemotes();
    }

    @Override
    public PaxosResponses<BooleanPaxosResponse> accept(long seq, PaxosProposal proposal) {
        return PaxosQuorumChecker.collectQuorumResponses(
                acceptors,
                acceptor -> acceptor.accept(seq, proposal),
                quorumSize,
                executors,
                PaxosQuorumChecker.DEFAULT_REMOTE_REQUESTS_TIMEOUT,
                cancelRemainingCalls).withoutRemotes();
    }

    @Override
    public PaxosResponses<PaxosLong> getLatestSequencePreparedOrAccepted() {
        return PaxosQuorumChecker.<PaxosAcceptor, PaxosLong>collectQuorumResponses(
                acceptors,
                acceptor -> ImmutablePaxosLong.of(acceptor.getLatestSequencePreparedOrAccepted()),
                quorumSize,
                executors,
                PaxosQuorumChecker.DEFAULT_REMOTE_REQUESTS_TIMEOUT,
                cancelRemainingCalls).withoutRemotes();
    }
}
