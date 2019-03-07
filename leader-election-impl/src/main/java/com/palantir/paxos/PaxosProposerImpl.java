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

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;

/**
 * Implementation of a paxos proposer than can be a designated proposer (leader) and designated
 * learner (informer).
 *
 * @author rullman
 */
public final class PaxosProposerImpl implements PaxosProposer {
    private static final Logger log = LoggerFactory.getLogger(PaxosProposerImpl.class);

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

    public static PaxosProposer newProposer(
            PaxosLearner localLearner,
            List<PaxosAcceptor> allAcceptors,
            List<PaxosLearner> allLearners,
            int quorumSize,
            UUID leaderUuid,
            ExecutorService executor) {
        return new PaxosProposerImpl(
                localLearner,
                allAcceptors,
                allLearners,
                quorumSize,
                leaderUuid.toString(),
                executor);
    }

    final ImmutableList<PaxosAcceptor> allAcceptors;
    final ImmutableList<PaxosLearner> allLearners;
    final PaxosLearner localLearner;
    final int quorumSize;
    final String uuid;
    final AtomicLong proposalNumber;

    private final ExecutorService executor;

    private PaxosProposerImpl(PaxosLearner localLearner,
                              List<PaxosAcceptor> acceptors,
                              List<PaxosLearner> learners,
                              int quorumSize,
                              String uuid,
                              ExecutorService executor) {
        Preconditions.checkState(
                quorumSize > acceptors.size() / 2,
                "quorum size needs to be at least the majority of acceptors");
        this.localLearner = localLearner;
        this.allAcceptors = ImmutableList.copyOf(acceptors);
        this.allLearners = ImmutableList.copyOf(learners);
        this.quorumSize = quorumSize;
        this.uuid = uuid;
        this.proposalNumber = new AtomicLong();
        this.executor = executor;
    }

    @Override
    public byte[] propose(final long seq, @Nullable byte[] bytes) throws PaxosRoundFailureException {
        final PaxosProposalId proposalId = new PaxosProposalId(proposalNumber.incrementAndGet(), uuid);
        PaxosValue toPropose = new PaxosValue(uuid, seq, bytes);

        // paxos phase one (prepare and promise)
        final PaxosValue finalValue = phaseOne(seq, proposalId, toPropose);

        // paxos phase two (accept request and accepted)
        phaseTwo(seq, proposalId, finalValue);

        // broadcast learned value
        for (final PaxosLearner learner : allLearners) {
            // local learner is forced to update later
            if (localLearner == learner) {
                continue;
            }

            executor.execute(() -> {
                try {
                    learner.learn(seq, finalValue);
                } catch (Throwable e) {
                    log.warn("Failed to teach learner the value {} at sequence {}",
                            UnsafeArg.of("value", bytes),
                            SafeArg.of("sequence", seq),
                            e);
                }
            });
        }

        // force local learner to update
        localLearner.learn(seq, finalValue);

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
        PaxosResponses<PaxosPromise> receivedPromises = PaxosQuorumChecker.collectQuorumResponses(
                allAcceptors,
                acceptor -> acceptor.prepare(seq, proposalId),
                quorumSize,
                executor,
                PaxosQuorumChecker.DEFAULT_REMOTE_REQUESTS_TIMEOUT);

        if (!receivedPromises.hasQuorum()) {
            // update proposal number on failure
            long maxProposal = receivedPromises.getResponses().stream()
                    .mapToLong(promise -> promise.promisedId.number)
                    .max()
                    .getAsLong();
            proposalNumber.getAndUpdate(currentNumber -> maxProposal > currentNumber ? maxProposal : currentNumber);
            throw new PaxosRoundFailureException("failed to acquire quorum in paxos phase one");
        }

        PaxosPromise greatestPromise = Collections.max(receivedPromises.getResponses());
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
        PaxosResponses<PaxosResponse> responses = PaxosQuorumChecker.collectQuorumResponses(
                allAcceptors,
                acceptor -> acceptor.accept(seq, proposal),
                quorumSize,
                executor,
                PaxosQuorumChecker.DEFAULT_REMOTE_REQUESTS_TIMEOUT);
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
