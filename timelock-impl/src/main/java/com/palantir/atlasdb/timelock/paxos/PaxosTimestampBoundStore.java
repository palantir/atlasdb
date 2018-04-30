/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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
import com.palantir.leader.NotCurrentLeaderException;
import com.palantir.logsafe.SafeArg;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosProposer;
import com.palantir.paxos.PaxosQuorumChecker;
import com.palantir.paxos.PaxosResponse;
import com.palantir.paxos.PaxosRoundFailureException;
import com.palantir.paxos.PaxosValue;
import com.palantir.timestamp.DebugLogger;
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

    private final ExecutorService executor = PTExecutors.newCachedThreadPool(
            PTExecutors.newNamedThreadFactory(true));

    public PaxosTimestampBoundStore(PaxosProposer proposer,
            PaxosLearner knowledge,
            List<PaxosAcceptor> acceptors,
            List<PaxosLearner> learners,
            long maximumWaitBeforeProposalMs) {
        DebugLogger.logger.info("Creating PaxosTimestampBoundStore. The UUID of my proposer is {}."
                + " Currently, I believe the timestamp bound is {}.",
                SafeArg.of("proposerUuid", proposer.getUuid()),
                SafeArg.of("timestampBound", knowledge.getGreatestLearnedValue()));
        this.proposer = proposer;
        this.knowledge = knowledge;
        this.acceptors = acceptors;
        this.learners = learners;
        this.maximumWaitBeforeProposalMs = maximumWaitBeforeProposalMs;
    }

    /**
     * Contacts a quorum of nodes to find the latest sequence number prepared or accepted from acceptors,
     * and the bound associated with this sequence number. This method MUST be called at least once before
     * storeUpperLimit() is called for the first time.
     *
     * @return the upper limit the cluster has agreed on
     * @throws ServiceNotAvailableException if we couldn't contact a quorum
     */
    @Override
    public synchronized long getUpperLimit() {
        List<PaxosLong> responses = getLatestSequenceNumbersFromAcceptors();
        PaxosLong max = Ordering.natural().onResultOf(PaxosLong::getValue).max(responses);
        agreedState = getAgreedState(max.getValue());
        return agreedState.getBound();
    }

    /**
     * Contacts all acceptors and gets the latest sequence number prepared or accepted by any of them.
     * This method only returns the values obtained if we obtained a quorum of values.
     *
     * @return latest sequence number prepared or accepted by any acceptor
     * @throws ServiceNotAvailableException if we couldn't contact a quorum
     */
    private List<PaxosLong> getLatestSequenceNumbersFromAcceptors() {
        List<PaxosLong> responses = PaxosQuorumChecker.collectQuorumResponses(
                ImmutableList.copyOf(acceptors),
                acceptor -> ImmutablePaxosLong.of(acceptor.getLatestSequencePreparedOrAccepted()),
                proposer.getQuorumSize(),
                executor,
                PaxosQuorumChecker.DEFAULT_REMOTE_REQUESTS_TIMEOUT_IN_SECONDS,
                ONLY_LOG_ON_QUORUM_FAILURE);
        if (!PaxosQuorumChecker.hasQuorum(responses, proposer.getQuorumSize())) {
            throw new ServiceNotAvailableException("could not get a quorum");
        }
        return responses;
    }

    /**
     * Obtains agreement for a given sequence number, pulling in values from previous sequence numbers
     * if needed.
     *
     * The semantics of this method are as follows:
     *  - If any learner knows that a value has already been agreed for this sequence number, return said value.
     *  - Otherwise, poll learners for the state of the previous sequence number.
     *     - If this is unavailable, the cluster must have agreed on (seq - 2), so read it and then force (seq - 1)
     *       to that value.
     *  - Finally, force agreement for seq to be the same value as that agreed for (seq - 1).
     *
     * This method has a precondition that (seq - 2) must be agreed upon; note that numbers up to and including
     * PaxosAcceptor.NO_LOG_ENTRY are always considered agreed upon.
     *
     * @param seq Sequence number to obtain agreement on
     * @return Sequence and bound for the given sequence number; guaranteed nonnull
     */
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

    /**
     * Forces agreement to be reached for a given sequence number; if the cluster hasn't reached agreement yet,
     * attempts to propose a given value. This method only returns when a value has been agreed upon for the provided
     * sequence number (though there are no guarantees as to whether said value is proposed by this node).
     *
     * The semantics of this method are as follows:
     *  - If any learner knows that a value has already been agreed for this sequence number, return said value.
     *  - Otherwise, propose the value oldState to the cluster. This call returns the value accepted by a
     *    quorum of nodes; return that value.
     *
     * Callers of this method that supply a null oldState are responsible for ensuring that the cluster has already
     * agreed on a value with the provided sequence number.
     *
     * @param seq Sequence number to obtain agreement on
     * @param oldState Value to propose, provided no learner has learned a value for this sequence number
     * @return Sequence and bound for the given sequence number; guaranteed nonnull
     * @throws NullPointerException if oldState is null and the cluster hasn't agreed on a value for seq yet
     */
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
     * Gets the timestamp bound learned for a given sequence number by polling all learners. Note that it suffices
     * to receive the value from a single learner, because Paxos guarantees that learners will not learn
     * different values for a given sequence number.
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

    /**
     * Proposes a new timestamp limit, with sequence number 1 greater than the current agreed bound, or
     * PaxosAcceptor.NO_LOG_ENTRY + 1 if nothing has been proposed or accepted yet.
     *
     * @param limit the new upper limit to be stored
     * @throws IllegalArgumentException if trying to persist a limit smaller than the agreed limit
     * @throws NotCurrentLeaderException if the timestamp limit has changed out from under us
     */
    @Override
    public synchronized void storeUpperLimit(long limit) throws MultipleRunningTimestampServiceError {
        long newSeq = PaxosAcceptor.NO_LOG_ENTRY + 1;
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
                if (newLimit < limit) {
                    // The bound is ours, but is not high enough.
                    // This is dangerous; proposing at the next sequence number is unsafe, as timestamp services
                    // generally assume they have the ALLOCATION_BUFFER_SIZE timestamps up to this.
                    // TODO (jkong): Devise a method that better preserves availability of the cluster.
                    log.warn("It appears we updated the timestamp limit to {}, which was less than our target {}."
                            + " This suggests we have another timestamp service running; possibly because we"
                            + " lost and regained leadership. For safety, we are now stopping this service.",
                            SafeArg.of("newLimit", newLimit),
                            SafeArg.of("target", limit));
                    throw new NotCurrentLeaderException(String.format(
                            "We updated the timestamp limit to %s, which was less than our target %s.",
                            newLimit,
                            limit));
                }
                return;
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
     * @throws NotCurrentLeaderException if the agreed timestamp bound (PaxosValue) changed under us
     */
    private void checkAgreedBoundIsOurs(long limit, long newSeq, PaxosValue value)
            throws NotCurrentLeaderException {
        if (!value.getLeaderUUID().equals(proposer.getUuid())) {
            String errorMsg = String.format(
                    "Timestamp limit changed from under us for sequence '%s' (proposer with UUID '%s' changed"
                            + " it, our UUID is '%s'). This suggests that we have lost leadership, and another timelock"
                            + " server has gained leadership and updated the timestamp bound."
                            + " The offending bound was '%s'; we tried to propose"
                            + " a bound of '%s'. (The offending Paxos value was '%s'.)",
                    newSeq,
                    value.getLeaderUUID(),
                    proposer.getUuid(),
                    PtBytes.toLong(value.getData()),
                    limit,
                    value);
            throw new NotCurrentLeaderException(errorMsg);
        }
        DebugLogger.logger.info("Trying to store limit '{}' for sequence '{}' yielded consensus on the value '{}'.",
                SafeArg.of("limit", limit),
                SafeArg.of("paxosSequenceNumber", newSeq),
                SafeArg.of("paxosValue", value));
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
                + " will retry in {} ms.",
                SafeArg.of("backoffTime", backoffTime),
                paxosException);
        try {
            backoffAction.backoff(backoffTime);
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Generates a random amount of time to wait for, in milliseconds.
     * This typically depends on the configuration of the Paxos algorithm; currently, we have implemented
     * this as U(1, k) where k is the maximum wait before proposal in the Paxos configuration.
     *
     * @return the amount of time to wait for, in milliseconds
     */
    private long getRandomBackoffTime() {
        return (long) (maximumWaitBeforeProposalMs * Math.random() + 1);
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
        void backoff(long backoffTime) throws InterruptedException;
    }
}
