/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

/**
 * Implementation of a paxos proposer than can be a designated proposer (leader) and designated
 * learner (informer).
 *
 * @author rullman
 */
public final class PaxosProposerImpl implements PaxosProposer {

    private final PaxosAcceptorNetworkClient acceptorClient;
    private final PaxosLearnerNetworkClient learnerClient;
    private final int quorumSize;
    private final String uuid;
    private final AtomicLong proposalNumber;

    /**
     * @deprecated use {@link #newProposer(PaxosLearner, List, List, int, UUID, ExecutorService)}.
     */
    @Deprecated
    public static PaxosProposer newProposer(
            PaxosLearner localLearner,
            List<PaxosAcceptor> allAcceptors,
            List<PaxosLearner> allLearners,
            int quorumSize,
            ExecutorService executor) {
        return newProposer(
                localLearner,
                allAcceptors,
                allLearners,
                quorumSize,
                UUID.randomUUID(),
                executor
        );
    }

    /**
     * @deprecated use {@link #newProposer(PaxosAcceptorNetworkClient, PaxosLearnerNetworkClient, int, UUID)} instead.
     */
    @Deprecated
    public static PaxosProposer newProposer(
            PaxosLearner knowledge,
            List<PaxosAcceptor> allAcceptors,
            List<PaxosLearner> allLearners,
            int quorumSize,
            UUID leaderUuid,
            ExecutorService singleExecutorService) {
        SingleLeaderAcceptorNetworkClient acceptorClient = new SingleLeaderAcceptorNetworkClient(
                allAcceptors,
                quorumSize,
                Maps.asMap(ImmutableSet.copyOf(allAcceptors), $ -> singleExecutorService));

        SingleLeaderLearnerNetworkClient learnerClient = new SingleLeaderLearnerNetworkClient(
                knowledge,
                allLearners.stream().filter(learner -> !learner.equals(knowledge)).collect(Collectors.toList()),
                quorumSize,
                Maps.asMap(ImmutableSet.copyOf(allLearners), $ -> singleExecutorService));

        return newProposer(acceptorClient, learnerClient, quorumSize, leaderUuid);
    }

    public static PaxosProposer newProposer(
            PaxosAcceptorNetworkClient acceptorClient,
            PaxosLearnerNetworkClient learnerClient,
            int quorumSize,
            UUID leaderUuid) {
        return new PaxosProposerImpl(acceptorClient, learnerClient, quorumSize, leaderUuid);
    }

    private PaxosProposerImpl(
            PaxosAcceptorNetworkClient acceptorClient,
            PaxosLearnerNetworkClient learnerClient,
            int quorumSize,
            UUID leaderUuid) {
        this.acceptorClient = acceptorClient;
        this.learnerClient = learnerClient;
        this.quorumSize = quorumSize;
        this.uuid = leaderUuid.toString();
        this.proposalNumber = new AtomicLong();
    }

    @Override
    public byte[] propose(final long seq, @Nullable byte[] bytes) throws PaxosRoundFailureException {
        return proposeWithId(uuid, seq, bytes);
    }

    @Override
    public byte[] proposeAnonymously(long seq, @Nullable byte[] proposalValue) throws PaxosRoundFailureException {
        return proposeWithId(UUID.randomUUID().toString(), seq, proposalValue);
    }

    private byte[] proposeWithId(
            String uuidToProposeWith,
            final long seq,
            @Nullable byte[] bytes) throws PaxosRoundFailureException {
        final PaxosProposalId proposalId = new PaxosProposalId(proposalNumber.incrementAndGet(), uuidToProposeWith);
        PaxosValue toPropose = new PaxosValue(uuidToProposeWith, seq, bytes);

        // paxos phase one (prepare and promise)
        final PaxosValue finalValue = phaseOne(seq, proposalId, toPropose);

        // paxos phase two (accept request and accepted)
        phaseTwo(seq, proposalId, finalValue);

        learnerClient.learn(seq, finalValue);

        return finalValue.getData();
    }

    /**
     * Executes phase one of paxos (see http://en.wikipedia.org/wiki/Paxos_(computer_science)#Basic_Paxos).
     *
     * @param seq the number identifying this instance of paxos
     * @param proposalId the id of the proposal currently being considered
     * @param proposalValue the default proposal value if no member of the quorum has already
     *        accepted an offer
     * @return the value accepted by the quorum
     * @throws PaxosRoundFailureException if quorum cannot be reached in this phase
     */
    private PaxosValue phaseOne(final long seq, final PaxosProposalId proposalId, PaxosValue proposalValue)
            throws PaxosRoundFailureException {
        PaxosResponses<PaxosPromise> receivedPromises = acceptorClient.prepare(seq, proposalId);

        if (!receivedPromises.hasQuorum()) {
            // update proposal number on failure
            long maxProposal = receivedPromises.stream()
                    .mapToLong(promise -> promise.promisedId.number)
                    .max()
                    .orElseGet(proposalNumber::get);
            proposalNumber.getAndUpdate(currentNumber -> maxProposal > currentNumber ? maxProposal : currentNumber);
            throw new PaxosRoundFailureException("failed to acquire quorum in paxos phase one");
        }

        PaxosPromise greatestPromise = Collections.max(receivedPromises.get());
        if (greatestPromise.lastAcceptedValue != null) {
            return greatestPromise.lastAcceptedValue;
        }

        return proposalValue;
    }

    /**
     * Executes phase two of paxos (see http://en.wikipedia.org/wiki/Paxos_(computer_science)#Basic_Paxos).
     *
     * @param seq the number identifying this instance of paxos
     * @param proposalId the id of the proposal currently being considered
     * @param proposalValue the value agree on in phase one of paxos
     * @throws PaxosRoundFailureException if quorum cannot be reached in this phase
     */
    private void phaseTwo(final long seq, PaxosProposalId proposalId, PaxosValue proposalValue)
            throws PaxosRoundFailureException {
        final PaxosProposal proposal = new PaxosProposal(proposalId, proposalValue);

        PaxosResponses<BooleanPaxosResponse> responses = acceptorClient.accept(seq, proposal);
        if (!responses.hasQuorum()) {
            throw new PaxosRoundFailureException("failed to acquire quorum in paxos phase two");
        }
    }

    @Override
    public int getQuorumSize() {
        return quorumSize;
    }

    @Override
    public String getUuid() {
        return uuid;
    }

}
