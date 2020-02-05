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

package com.palantir.atlasdb.timelock.paxos.db;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;
import com.palantir.atlasdb.timelock.paxos.PaxosQuorumCheckingCoalescingFunction.PaxosContainer;
import com.palantir.common.remoting.ServiceNotAvailableException;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosAcceptorNetworkClient;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosLearnerNetworkClient;
import com.palantir.paxos.PaxosLong;
import com.palantir.paxos.PaxosProposer;
import com.palantir.paxos.PaxosResponses;
import com.palantir.paxos.PaxosRoundFailureException;
import com.palantir.paxos.PaxosValue;

public class PaxosAtomicValue<T> implements AtomicValue<T> {
    private static final Logger log = LoggerFactory.getLogger(PaxosAtomicValue.class);

    private final PaxosProposer proposer;
    private final PaxosAcceptorNetworkClient acceptor;
    private final PaxosLearner knowledge;
    private final PaxosLearnerNetworkClient learnerClient;
    private final long maximumWaitBeforeProposalMs;
    private final Function<T, byte[]> serialize;
    private final Function<byte[], T> deserialize;
    private final T defaultValue;

    public PaxosAtomicValue(PaxosProposer proposer, PaxosAcceptorNetworkClient acceptor,
            PaxosLearner knowledge, PaxosLearnerNetworkClient learnerClient,
            long maximumWaitBeforeProposalMs, Function<T, byte[]> serialize,
            Function<byte[], T> deserialize, T defaultValue) {
        this.proposer = proposer;
        this.acceptor = acceptor;
        this.knowledge = knowledge;
        this.learnerClient = learnerClient;
        this.maximumWaitBeforeProposalMs = maximumWaitBeforeProposalMs;
        this.serialize = serialize;
        this.deserialize = deserialize;
        this.defaultValue = defaultValue;
    }

    @Override
    public T get() {
        return getAgreedState(getSequenceNumber());
    }

    @Override
    public synchronized boolean compareAndSet(T expected, T update) {
        long seq = getSequenceNumber();
        T present = getAgreedState(seq);
        if (!present.equals(expected)) {
            return false;
        }
        long newSeq = seq + 1;
        while (true) {
            try {
                proposer.propose(newSeq, serialize.apply(update));
                PaxosValue value = knowledge.getLearnedValue(newSeq)
                        .orElseThrow(() -> new SafeIllegalStateException("Paxos proposal"
                                + " returned without learning a value. This is unexpected and would suggest a bug in"
                                + " AtlasDB code. Please contact support."));
                return value.getLeaderUUID().equals(proposer.getUuid());
            } catch (PaxosRoundFailureException e) {
                waitForRandomBackoff(e, this::wait);
            }
        }
    }

    private long getSequenceNumber() {
        List<PaxosLong> responses = getLatestSequenceNumbersFromAcceptors();
        PaxosLong max = Ordering.natural().onResultOf(PaxosLong::getValue).max(responses);
        return max.getValue();
    }

    /**
     * Contacts all acceptors and gets the latest sequence number prepared or accepted by any of them. This method only
     * returns the values obtained if we obtained a quorum of values.
     *
     * @return latest sequence number prepared or accepted by any acceptor
     * @throws ServiceNotAvailableException if we couldn't contact a quorum
     */
    private List<PaxosLong> getLatestSequenceNumbersFromAcceptors() {
        PaxosResponses<PaxosLong> responses = acceptor.getLatestSequencePreparedOrAccepted();
        if (!responses.hasQuorum()) {
            throw new ServiceNotAvailableException("could not get a quorum");
        }
        return responses.get();
    }

    /**
     * Obtains agreement for a given sequence number, pulling in values from previous sequence numbers if needed.
     * <p>
     * The semantics of this method are as follows: - If any learner knows that a value has already been agreed for this
     * sequence number, return said value. - Otherwise, poll learners for the state of the previous sequence number. -
     * If this is unavailable, the cluster must have agreed on (seq - 2), so read it and then force (seq - 1) to that
     * value. - Finally, force agreement for seq to be the same value as that agreed for (seq - 1).
     * <p>
     * This method has a precondition that (seq - 2) must be agreed upon; note that numbers up to and including
     * PaxosAcceptor.NO_LOG_ENTRY are always considered agreed upon.
     *
     * @param seq Sequence number to obtain agreement on
     * @return Sequence and bound for the given sequence number; guaranteed nonnull
     */
    @VisibleForTesting
    T getAgreedState(long seq) {
        final Optional<T> state = getLearnedState(seq);
        if (state.isPresent()) {
            return state.get();
        }

        // In the common case seq - 1 will be agreed upon before seq is prepared.
        Optional<T> lastState = getLearnedState(seq - 1);
        if (!lastState.isPresent()) {
            // We know that even in the case of a truncate, seq - 2 will always be agreed upon.
            T forced = forceAgreedState(seq - 2, null);
            lastState = Optional.of(forceAgreedState(seq - 1, forced));
        }

        return forceAgreedState(seq, lastState.get());
    }


    /**
     * Forces agreement to be reached for a given sequence number; if the cluster hasn't reached agreement yet, attempts
     * to propose a given value. This method only returns when a value has been agreed upon for the provided sequence
     * number (though there are no guarantees as to whether said value is proposed by this node).
     * <p>
     * The semantics of this method are as follows: - If any learner knows that a value has already been agreed for this
     * sequence number, return said value. - Otherwise, propose the value oldState to the cluster. This call returns the
     * value accepted by a quorum of nodes; return that value.
     * <p>
     * Callers of this method that supply a null oldState are responsible for ensuring that the cluster has already
     * agreed on a value with the provided sequence number.
     *
     * @param seq      Sequence number to obtain agreement on
     * @param oldState Value to propose, provided no learner has learned a value for this sequence number
     * @return Sequence and bound for the given sequence number; guaranteed nonnull
     * @throws NullPointerException if oldState is null and the cluster hasn't agreed on a value for seq yet
     */
    @VisibleForTesting
    T forceAgreedState(long seq, @Nullable T oldState) {
        if (seq <= PaxosAcceptor.NO_LOG_ENTRY) {
            return defaultValue;
        }

        Optional<T> state = getLearnedState(seq);
        if (state.isPresent()) {
            return state.get();
        }

        while (true) {
            try {
                byte[] acceptedValue = proposer.propose(seq, oldState == null ? null : serialize.apply(oldState));
                // propose must never return null.  We only pass in null for things we know are agreed upon already.
                Preconditions.checkNotNull(acceptedValue, "Proposed value can't be null, but was in sequence %s", seq);
                return deserialize.apply(acceptedValue);
            } catch (PaxosRoundFailureException e) {
                waitForRandomBackoff(e, Thread::sleep);
            }
        }
    }

    /**
     * Gets the timestamp bound learned for a given sequence number by polling all learners. Note that it suffices to
     * receive the value from a single learner, because Paxos guarantees that learners will not learn different values
     * for a given sequence number.
     *
     * @param seq The sequence number to poll the learners for
     * @return Sequence ID and bound for the specified sequence number, or an empty Optional if we cannot connect to any
     * learner which knows a value for this sequence number
     */
    private Optional<T> getLearnedState(long seq) {
        if (seq <= PaxosAcceptor.NO_LOG_ENTRY) {
            return Optional.of(defaultValue);
        }

        PaxosResponses<PaxosContainer<Optional<T>>> responses =
                learnerClient.getLearnedValue(seq,
                        maybeValue -> PaxosContainer.of(maybeValue
                                .map(PaxosValue::getData)
                                .map(deserialize)));

        return responses.stream()
                .map(PaxosContainer::get)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
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

    private interface BackoffAction {
        void backoff(long backoffTime) throws InterruptedException;
    }
}
