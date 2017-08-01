/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.timelock.coordination;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import javax.annotation.concurrent.GuardedBy;

import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.remoting.ServiceNotAvailableException;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosProposer;
import com.palantir.paxos.PaxosQuorumChecker;
import com.palantir.paxos.PaxosResponse;
import com.palantir.paxos.PaxosRoundFailureException;
import com.palantir.paxos.PaxosValue;
import com.palantir.remoting2.tracing.Tracers;
import com.palantir.timelock.partition.Assignment;
import com.palantir.timelock.partition.PaxosPartitionService;

public class PaxosCoordinationService implements CoordinationService {
    private static final Logger log = LoggerFactory.getLogger(PaxosPartitionService.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new GuavaModule())
            .registerModule(new Jdk8Module());

    private final PaxosProposer proposer;
    private final PaxosAcceptor ourAcceptor;
    private final PaxosLearner knowledge;

    private final List<PaxosAcceptor> acceptors;
    private final List<PaxosLearner> learners;

    private final ExecutorService executor = Tracers.wrap(PTExecutors.newCachedThreadPool(
            PTExecutors.newNamedThreadFactory(true)));

    @GuardedBy("this")
    private Assignment reference;
    @GuardedBy("this")
    private SequenceAndAssignment agreedState;

    public PaxosCoordinationService(
            PaxosProposer proposer,
            PaxosAcceptor ourAcceptor,
            PaxosLearner knowledge,
            List<PaxosAcceptor> acceptors,
            List<PaxosLearner> learners) {
        // TODO (jkong): Support less restrictive coordination.
        Preconditions.checkState(proposer.getQuorumSize() == acceptors.size(),
                "The coordination service assumes a consistency of ALL.");
        this.proposer = proposer;
        this.ourAcceptor = ourAcceptor;
        this.knowledge = knowledge;
        this.acceptors = acceptors;
        this.learners = learners;

        this.agreedState = ImmutableSequenceAndAssignment.of(PaxosAcceptor.NO_LOG_ENTRY, Assignment.nopAssignment());
    }

    @Override
    public synchronized SequenceAndAssignment getCoordinatedValue() {
        // This MUST be read before the value from the acceptor.
        PaxosValue greatestKnown = knowledge.getGreatestLearnedValue();
        long knowledgeSeq = greatestKnown == null ? PaxosAcceptor.NO_LOG_ENTRY : greatestKnown.getRound();

        if (ourAcceptor.getLatestSequencePreparedOrAccepted() == knowledgeSeq) {
            // Since we require consistency all, we are up to date.
            if (agreedState.sequenceNumber() != knowledgeSeq) {
                SequenceAndAssignment oldState = agreedState;
                agreedState = ImmutableSequenceAndAssignment.of(
                        knowledgeSeq,
                        decodeAssignment(greatestKnown.getData()));
                log.info("Updated agreed state from {} to {}",
                        SafeArg.of("oldState", oldState),
                        SafeArg.of("newState", agreedState));
            }
            return agreedState;
        }

        // In this case we actually need to read the world.
        return readAssignment();
    }

    private SequenceAndAssignment readAssignment() {
        return getAgreedState(getLatestSequenceNumber());
    }

    private long getLatestSequenceNumber() {
        List<PaxosLong> sequences = getLatestSequenceNumbersFromAcceptors();
        try {
            return Ordering.natural().onResultOf(PaxosLong::value).max(sequences).value();
        } catch (NoSuchElementException e) {
            return PaxosAcceptor.NO_LOG_ENTRY;
        }
    }

    private List<PaxosLong> getLatestSequenceNumbersFromAcceptors() {
        List<PaxosLong> responses = PaxosQuorumChecker.collectQuorumResponses(
                ImmutableList.copyOf(acceptors),
                acceptor -> ImmutablePaxosLong.of(acceptor.getLatestSequencePreparedOrAccepted()),
                proposer.getQuorumSize(),
                executor,
                PaxosQuorumChecker.DEFAULT_REMOTE_REQUESTS_TIMEOUT_IN_SECONDS,
                false);
        if (!PaxosQuorumChecker.hasQuorum(responses, proposer.getQuorumSize())) {
            throw new ServiceNotAvailableException("could not get a quorum");
        }
        return responses;
    }

    private SequenceAndAssignment getAgreedState(long seq) {
        final Optional<SequenceAndAssignment> state = getLearnedState(seq);
        if (state.isPresent()) {
            return state.get();
        }
        // If nothing was learned yet...
        return blockOnAgreement(seq, Assignment.nopAssignment());
    }

    private Optional<SequenceAndAssignment> getLearnedState(long seq) {
        if (seq <= PaxosAcceptor.NO_LOG_ENTRY) {
            // TODO(jkong): Nothing agreed on yet.
            return Optional.of(ImmutableSequenceAndAssignment.of(seq, Assignment.nopAssignment()));
        }

        // quorum of 1 is ok, because learners are authoritative
        List<PaxosAssignment> responses = PaxosQuorumChecker.collectQuorumResponses(
                ImmutableList.copyOf(learners),
                learner -> ImmutablePaxosAssignment.of(decodeAssignment(learner.getLearnedValue(seq).getData())),
                1,
                executor,
                PaxosQuorumChecker.DEFAULT_REMOTE_REQUESTS_TIMEOUT_IN_SECONDS,
                false);
        if (responses.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(ImmutableSequenceAndAssignment.of(seq, responses.iterator().next().assignment()));
    }

    private SequenceAndAssignment blockOnAgreement(long seq, Assignment value) {
        if (seq <= PaxosAcceptor.NO_LOG_ENTRY) {
            return ImmutableSequenceAndAssignment.of(seq, Assignment.nopAssignment());
        }

        Optional<SequenceAndAssignment> state = getLearnedState(seq);
        if (state.isPresent()) {
            return state.get();
        }

        while (true) {
            try {
                byte[] acceptedValue = proposer.propose(seq, encodeAssignment(value));

                // Not necessarily equivalent to the value
                return ImmutableSequenceAndAssignment.of(seq, decodeAssignment(acceptedValue));
            } catch (PaxosRoundFailureException e) {
                try {
                    // TODO (jkong): backoff properly
                    this.wait(1000);
                } catch (InterruptedException e1) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    @Override
    public synchronized SequenceAndAssignment proposeAssignment(Assignment assignment) {
        long newSeq = getLatestSequenceNumber() + 1;
        log.info("Trying to propose {} at sequence {}",
                SafeArg.of("assignment", assignment),
                SafeArg.of("sequence", newSeq));

        while (true) {
            try {
                proposer.propose(newSeq, encodeAssignment(assignment));
                PaxosValue value = knowledge.getLearnedValue(newSeq);
                Assignment actualAssignment = decodeAssignment(value.getData());
                agreedState = ImmutableSequenceAndAssignment.of(newSeq, actualAssignment);
                log.info("Reached consensus on {} at sequence {}",
                        SafeArg.of("assignment", actualAssignment),
                        SafeArg.of("sequence", newSeq));
                return agreedState;
            } catch (PaxosRoundFailureException e) {
                newSeq = getLatestSequenceNumber();
                try {
                    // TODO (jkong): backoff properly
                    this.wait(1000);
                } catch (InterruptedException e1) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private static Assignment decodeAssignment(byte[] bytes) {
        try {
            return OBJECT_MAPPER.readValue(bytes, Assignment.class);
        } catch (IOException e) {
            log.warn("Error deserializing an Assignment from the byte-array {}.",
                    UnsafeArg.of("byte-array", bytes));
            throw Throwables.propagate(e);
        }
    }

    private static byte[] encodeAssignment(Assignment assignment) {
        try {
            return OBJECT_MAPPER.writeValueAsBytes(assignment);
        } catch (JsonProcessingException e) {
            throw Throwables.propagate(e);
        }
    }

    @Value.Immutable
    interface PaxosLong extends PaxosResponse {
        @Override
        default boolean isSuccessful() {
            return true;
        }

        @Value.Parameter
        long value();
    }

    @Value.Immutable
    interface PaxosAssignment extends PaxosResponse {
        @Override
        default boolean isSuccessful() {
            return true;
        }

        @Value.Parameter
        Assignment assignment();
    }
}
