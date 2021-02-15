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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * {@link PaxosAcceptorNetworkClient} encapsulates the consensus portion of the request and involves communicating with
 * multiple {@link PaxosAcceptor}s. This should be used over {@link PaxosQuorumChecker} and {@link PaxosAcceptor} where
 * possible. This allows us to specifically tailor our approach for single leader vs multi leader configurations.
 */
public interface PaxosAcceptorNetworkClient {

    /**
     * The acceptors prepare for a given proposal by either promising not to accept future proposals
     * or rejecting the proposal.
     *
     * @param seq the number identifying this instance of paxos
     * @param proposalId the proposal to prepare for
     * @return promises from the cluster not to accept lower numbered proposals
     */
    PaxosResponses<PaxosPromise> prepare(long seq, PaxosProposalId proposalId);

    /**
     * The acceptors decide whether to accept or reject a given proposal.
     *
     * @param seq the number identifying this instance of paxos
     * @param proposal the proposal in question
     * @return paxos messages indicating whether the cluster accepted or rejected the proposal
     */
    PaxosResponses<BooleanPaxosResponse> accept(long seq, PaxosProposal proposal);

    /**
     * The acceptors return the sequence number of their most recent known round.
     *
     * @return sequence numbers of each acceptor's most recent round. {@value PaxosAcceptor#NO_LOG_ENTRY} will be
     *         included if an acceptor has not prepared or accepted any rounds.
     */
    PaxosResponses<PaxosLong> getLatestSequencePreparedOrAccepted();

    default ListenableFuture<PaxosResponses<PaxosLong>> getLatestSequencePreparedOrAcceptedAsync() {
        return Futures.immediateFuture(getLatestSequencePreparedOrAccepted());
    }
}
