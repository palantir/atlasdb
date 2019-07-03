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

/**
 * {@link PaxosAcceptorNetworkClient}s encapsulates the consensus portion of the request, and this should be used over
 * {@link PaxosAcceptor}. This allows us to specifically tailor our approach for single leader vs multi leader
 * configurations.
 */
public interface PaxosAcceptorNetworkClient {

    /**
     * The acceptor prepares for a given proposal by either promising not to accept future proposals
     * or rejecting the proposal.
     *
     * @param seq the number identifying this instance of paxos
     * @param proposalId the proposal to prepare for
     * @return a paxos promise not to accept lower numbered proposals
     */
    PaxosResponses<PaxosPromise> prepare(long seq, PaxosProposalId proposalId);

    /**
     * The acceptor decides whether to accept or reject a given proposal.
     *
     * @param seq the number identifying this instance of paxos
     * @param proposal the proposal in question
     * @return a paxos message indicating if the proposal was accepted or rejected
     */
    PaxosResponses<BooleanPaxosResponse> accept(long seq, PaxosProposal proposal);

    /**
     * Gets the sequence number of the acceptor's most recent known round.
     *
     * @return the sequence number of the most recent round or {@value PaxosAcceptor#NO_LOG_ENTRY} if this
     *         acceptor has not prepared or accepted any rounds
     */
    PaxosResponses<PaxosLong> getLatestSequencePreparedOrAccepted();

}
