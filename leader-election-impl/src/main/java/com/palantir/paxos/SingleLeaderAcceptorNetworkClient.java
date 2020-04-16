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

public class SingleLeaderAcceptorNetworkClient implements PaxosAcceptorNetworkClient {

    private final PaxosExecutionEnvironment<PaxosAcceptor> environment;
    private final int quorumSize;
    private final boolean cancelRemainingCalls;

    public SingleLeaderAcceptorNetworkClient(PaxosExecutionEnvironment<PaxosAcceptor> environment,
            int quorumSize, boolean cancelRemainingCalls) {
        this.environment = environment;
        this.quorumSize = quorumSize;
        this.cancelRemainingCalls = cancelRemainingCalls;
    }

    @Override
    public PaxosResponses<PaxosPromise> prepare(long seq, PaxosProposalId proposalId) {
        return PaxosQuorumChecker.collectQuorumResponses(
                environment,
                acceptor -> acceptor.prepare(seq, proposalId),
                quorumSize,
                PaxosQuorumChecker.DEFAULT_REMOTE_REQUESTS_TIMEOUT,
                cancelRemainingCalls).withoutRemotes();
    }

    @Override
    public PaxosResponses<BooleanPaxosResponse> accept(long seq, PaxosProposal proposal) {
        return PaxosQuorumChecker.collectQuorumResponses(
                environment,
                acceptor -> acceptor.accept(seq, proposal),
                quorumSize,
                PaxosQuorumChecker.DEFAULT_REMOTE_REQUESTS_TIMEOUT,
                cancelRemainingCalls).withoutRemotes();
    }

    @Override
    public PaxosResponses<PaxosLong> getLatestSequencePreparedOrAccepted() {
        return PaxosQuorumChecker.<PaxosAcceptor, PaxosLong>collectQuorumResponses(
                environment,
                acceptor -> ImmutablePaxosLong.of(acceptor.getLatestSequencePreparedOrAccepted()),
                quorumSize,
                PaxosQuorumChecker.DEFAULT_REMOTE_REQUESTS_TIMEOUT,
                cancelRemainingCalls).withoutRemotes();
    }
}
