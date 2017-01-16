/**
 * Copyright 2016 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.timelock.paxos;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.remoting.ServiceNotAvailableException;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosProposer;
import com.palantir.paxos.PaxosQuorumChecker;
import com.palantir.paxos.PaxosResponse;
import com.palantir.paxos.PaxosRoundFailureException;
import com.palantir.paxos.PaxosValue;
import com.palantir.timestamp.MultipleRunningTimestampServiceError;
import com.palantir.timestamp.TimestampBoundStore;

public class PaxosTimestampBoundStore implements TimestampBoundStore {
    private static final Logger log = LoggerFactory.getLogger(PaxosTimestampBoundStore.class);

    private static final int QUORUM_OF_ONE = 1;
    private static final boolean ONLY_LOG_ON_QUORUM_FAILURE = true;

    private final PaxosProposer proposer;
    private final PaxosLearner knowledge;

    private final List<PaxosAcceptor> acceptors;
    private final List<PaxosLearner> learners;
    private final long maximumWaitBeforeProposalMs;

    @GuardedBy("this")
    private SequenceAndBound agreedState;

    private final ExecutorService executor = PTExecutors.newCachedThreadPool(PTExecutors.newNamedThreadFactory(true));

    public PaxosTimestampBoundStore(PaxosProposer proposer,
            PaxosLearner knowledge,
            List<PaxosAcceptor> acceptors,
            List<PaxosLearner> learners,
            long maximumWaitBeforeProposalMs) {
        this.proposer = proposer;
        this.knowledge = knowledge;
        this.acceptors = acceptors;
        this.learners = learners;
        this.maximumWaitBeforeProposalMs = maximumWaitBeforeProposalMs;
    }

    @Override
    public synchronized long getUpperLimit() {
        List<PaxosLong> responses = getLatestSequenceNumbersFromAcceptors();
        PaxosLong max = Ordering.natural().onResultOf(PaxosLong::getValue).max(responses);
        agreedState = getAgreedState(max.getValue());
        return agreedState.getBound();
    }

    private List<PaxosLong> getLatestSequenceNumbersFromAcceptors() {
        List<PaxosLong> responses = PaxosQuorumChecker.collectQuorumResponses(
                ImmutableList.copyOf(acceptors),
                acceptor -> ImmutablePaxosLong.of(acceptor.getLatestSequencePreparedOrAccepted()),
                proposer.getQuorumSize(),
                executor,
                PaxosQuorumChecker.DEFAULT_REMOTE_REQUESTS_TIMEOUT_IN_SECONDS,
                true);
        if (!PaxosQuorumChecker.hasQuorum(responses, proposer.getQuorumSize())) {
            throw new ServiceNotAvailableException("could not get a quorum");
        }
        return responses;
    }

    @VisibleForTesting
    SequenceAndBound getAgreedState(long seq) {
        final Optional<SequenceAndBound> state = getLearnedState(seq);
        if (state.isPresent()) {
            return state.get();
        }

        // In the common case seq - 1 will be agreed upon before seq is prepared.
        Optional<SequenceAndBound> lastState = getLearnedState(seq - 1);
        if (!lastState.isPresent()) {
            // We know that even in the case of a truncate, seq - 2 will always be agreed upon.
            SequenceAndBound forced = forceAgreedState(seq - 2, null);
            lastState = Optional.of(forceAgreedState(seq - 1, forced.getBound()));
        }

        return forceAgreedState(seq, lastState.get().getBound());
    }

    @VisibleForTesting
    SequenceAndBound forceAgreedState(long seq, @Nullable Long oldState) {
        if (seq <= PaxosAcceptor.NO_LOG_ENTRY) {
            return ImmutableSequenceAndBound.of(PaxosAcceptor.NO_LOG_ENTRY, 0L);
        }

        Optional<SequenceAndBound> state = getLearnedState(seq);
        if (state.isPresent()) {
            return state.get();
        }

        while (true) {
            try {
                byte[] acceptedValue = proposer.propose(seq, oldState == null ? null : PtBytes.toBytes(oldState));
                // propose must never return null.  We only pass in null for things we know are agreed upon already.
                Preconditions.checkNotNull(acceptedValue, "Proposed value can't be null, but was in sequence %s", seq);
                return ImmutableSequenceAndBound.of(seq, PtBytes.toLong(acceptedValue));
            } catch (PaxosRoundFailureException e) {
                waitForRandomBackoff(e, Thread::sleep);
            }
        }
    }

    /**
     * Gets the timestamp bound learned for a given sequence number by polling all learners.
     *
     * @param seq The sequence number to poll the learners for
     * @return Sequence ID and bound for the specified sequence number, or an empty Optional if we cannot connect
     * to any learner which knows a value for this sequence number
     */
    private Optional<SequenceAndBound> getLearnedState(long seq) {
        if (seq <= PaxosAcceptor.NO_LOG_ENTRY) {
            return Optional.of(ImmutableSequenceAndBound.of(PaxosAcceptor.NO_LOG_ENTRY, 0L));
        }
        List<PaxosLong> responses = PaxosQuorumChecker.collectQuorumResponses(
                ImmutableList.copyOf(learners),
                learner -> getLearnedValue(seq, learner),
                QUORUM_OF_ONE,
                executor,
                PaxosQuorumChecker.DEFAULT_REMOTE_REQUESTS_TIMEOUT_IN_SECONDS,
                ONLY_LOG_ON_QUORUM_FAILURE);
        if (responses.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(ImmutableSequenceAndBound.of(seq, responses.iterator().next().getValue()));
    }

    /**
     * Gets the value that has been learned from an individual PaxosLearner for a given sequence number.
     *
     * @param seq The sequence number to get a value for
     * @param learner The Paxos learner to query for a value
     * @return the value learned by the learner for seq
     * @throws NoSuchElementException if the learner has not learned any value for seq
     */
    private static PaxosLong getLearnedValue(long seq, PaxosLearner learner) {
        PaxosValue value = learner.getLearnedValue(seq);
        if (value == null) {
            throw new NoSuchElementException(
                    String.format("Tried to get a learned value for sequence number '%d' which didn't exist", seq));
        }
        return ImmutablePaxosLong.of(PtBytes.toLong(value.getData()));
    }

    @Override
    public synchronized void storeUpperLimit(long limit) throws MultipleRunningTimestampServiceError {
        long newSeq = 0;
        if (agreedState != null) {
            Preconditions.checkArgument(limit >= agreedState.getBound(),
                    "Tried to store an upper limit %s less than the current limit %s", limit, agreedState.getBound());
            newSeq = agreedState.getSeqId() + 1;
        }
        while (true) {
            try {
                proposer.propose(newSeq, PtBytes.toBytes(limit));
                PaxosValue value = knowledge.getLearnedValue(newSeq);
                checkAgreedBoundIsOurs(limit, newSeq, value);
                long newLimit = PtBytes.toLong(value.getData());
                agreedState = ImmutableSequenceAndBound.of(newSeq, newLimit);
                if (newLimit >= limit) {
                    return;
                }
            } catch (PaxosRoundFailureException e) {
                waitForRandomBackoff(e, this::wait);
            }
        }
    }

    /**
     * Checks that the PaxosValue agreed upon by a quorum of nodes in our cluster was proposed by us.
     *
     * @param limit the limit our node has proposed
     * @param newSeq the sequence number for which our node has proposed the limit
     * @param value PaxosValue agreed upon by a quorum of nodes, for sequence number newSeq
     * @throws MultipleRunningTimestampServiceError if the agreed timestamp bound (PaxosValue) changed under us
     */
    private void checkAgreedBoundIsOurs(long limit, long newSeq, PaxosValue value)
            throws MultipleRunningTimestampServiceError {
        if (!value.getLeaderUUID().equals(proposer.getUUID())) {
            String errorMsg = String.format(
                    "Timestamp limit changed from under us for sequence '%s' (leader with UUID '%s' changed"
                            + " it, our UUID is '%s'). This suggests that another timestamp store for this"
                            + " namespace is running. The offending bound was '%s'; we tried to propose"
                            + " a bound of '%s'. (The offending Paxos value was '%s'.)",
                    newSeq,
                    value.getLeaderUUID(),
                    proposer.getUUID(),
                    PtBytes.toLong(value.getData()),
                    limit,
                    value);
            throw new MultipleRunningTimestampServiceError(errorMsg);
        }
    }

    /**
     * Executes a backoff action which is given a random amount of time to wait in milliseconds. This is used in Paxos
     * to resolve multiple concurrent proposals. Users are allowed to specify their own backoff action,
     * to handle cases where users hold or do not hold monitor locks, for instance.
     *
     * @param paxosException the PaxosRoundFailureException that caused us to wait
     * @param backoffAction the action to take (which consumes the time, in milliseconds, to wait for)
     */
    private void waitForRandomBackoff(PaxosRoundFailureException paxosException, BackoffAction backoffAction) {
        long backoffTime = getRandomBackoffTime();
        log.info("Paxos proposal couldn't complete, because we could not connect to a quorum of nodes. We"
                + " will retry in {}ms. The Paxos exception was {}",
                backoffTime,
                paxosException);
        try {
            backoffAction.accept(backoffTime);
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Generates a random amount of time to wait for, in milliseconds.
     * This typically depends on the configuration of the Paxos algorithm; currently, we have implemented
     * this as U(0, k) where k is the maximum wait before proposal in the Paxos configuration.
     *
     * @return the amount of time to wait for, in milliseconds
     */
    private long getRandomBackoffTime() {
        return (long) (maximumWaitBeforeProposalMs * Math.random());
    }

    @Value.Immutable
    interface PaxosLong extends PaxosResponse {
        @Override
        default boolean isSuccessful() {
            return true;
        }

        @Value.Parameter
        long getValue();
    }

    @Value.Immutable
    interface SequenceAndBound {
        @Value.Parameter
        long getSeqId();

        @Value.Parameter
        long getBound();
    }

    private interface BackoffAction {
        void accept(long backoffTime) throws InterruptedException;
    }
}
