package com.palantir.atlasdb.server;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private final PaxosProposer proposer;
    private final PaxosLearner knowledge;

    private final List<PaxosAcceptor> acceptors;
    private final List<PaxosLearner> learners;

    @GuardedBy("this")
    private SequenceAndBound agreedState;

    private final ExecutorService executor = PTExecutors.newCachedThreadPool(PTExecutors.newNamedThreadFactory(true));

    public PaxosTimestampBoundStore(PaxosProposer proposer,
            PaxosLearner knowledge,
            List<PaxosAcceptor> acceptors,
            List<PaxosLearner> learners) {
        this.proposer = proposer;
        this.knowledge = knowledge;
        this.acceptors = acceptors;
        this.learners = learners;
    }

    @Override
    public synchronized long getUpperLimit() {
        List<PaxosLong> responses = PaxosQuorumChecker.collectQuorumResponses(
                ImmutableList.copyOf(acceptors),
                a -> ImmutablePaxosLong.of(a.getLatestSequencePreparedOrAccepted()),
                proposer.getQuorumSize(),
                executor,
                PaxosQuorumChecker.DEFAULT_REMOTE_REQUESTS_TIMEOUT_IN_SECONDS,
                true);
        if (!PaxosQuorumChecker.hasQuorum(responses, proposer.getQuorumSize())) {
            throw new ServiceNotAvailableException("could not get a quorum");
        }
        PaxosLong max = Ordering.natural().onResultOf((PaxosLong a) -> a.getValue()).max(responses);
        agreedState = getAgreedState(max.getValue());
        return agreedState.getBound();
    }

    private SequenceAndBound getAgreedState(long seq) {
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

    private SequenceAndBound forceAgreedState(long seq, @Nullable Long oldState) {
        if (seq <= PaxosAcceptor.NO_LOG_ENTRY) {
            return ImmutableSequenceAndBound.of(PaxosAcceptor.NO_LOG_ENTRY, 0L);
        }

        Optional<SequenceAndBound> state = getLearnedState(seq);
        if (state.isPresent()) {
            return state.get();
        }

        while (true) {
            try {
                byte[] value = proposer.propose(seq, oldState == null ? null : PtBytes.toBytes(oldState));
                // propose must never be null.  We only pass in null for things we know are agreed upon already.
                Preconditions.checkNotNull(value);
                return ImmutableSequenceAndBound.of(seq, PtBytes.toLong(value));
            } catch (PaxosRoundFailureException e) {
                log.info("failed during propose", e);
                long backoffTime = (long) (200.0 * Math.random());
                try {
                    Thread.sleep(backoffTime);
                } catch (InterruptedException e1) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private Optional<SequenceAndBound> getLearnedState(long seq) {
        if (seq <= PaxosAcceptor.NO_LOG_ENTRY) {
            return Optional.of(ImmutableSequenceAndBound.of(PaxosAcceptor.NO_LOG_ENTRY, 0L));
        }
        List<PaxosLong> responses = PaxosQuorumChecker.collectQuorumResponses(
                ImmutableList.copyOf(learners),
                l -> getLearnedValue(seq, l),
                1,
                executor,
                PaxosQuorumChecker.DEFAULT_REMOTE_REQUESTS_TIMEOUT_IN_SECONDS,
                true);
        if (responses.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(ImmutableSequenceAndBound.of(seq, responses.iterator().next().getValue()));
    }

    private static PaxosLong getLearnedValue(long seq, PaxosLearner l) {
        PaxosValue value = l.getLearnedValue(seq);
        if (value == null) {
            throw new NoSuchElementException();
        }
        return ImmutablePaxosLong.of(PtBytes.toLong(value.getData()));
    }

    @Override
    public synchronized void storeUpperLimit(long limit) throws MultipleRunningTimestampServiceError {
        Preconditions.checkArgument(limit >= agreedState.getBound());
        long newSeq = agreedState.getSeqId() + 1;
        while (true) {
            try {
                proposer.propose(newSeq, PtBytes.toBytes(limit));
                PaxosValue value = knowledge.getLearnedValue(newSeq);
                if (!value.getLeaderUUID().equals(proposer.getUUID())) {
                    throw new MultipleRunningTimestampServiceError("concurrent proposal: " + value);
                }
                long newLimit = PtBytes.toLong(value.getData());
                agreedState = ImmutableSequenceAndBound.of(newSeq, newLimit);
                if (newLimit >= limit) {
                    return;
                }
            } catch (PaxosRoundFailureException e) {
                log.info("failed during propose", e);
                long backoffTime = (long) (200.0 * Math.random());
                try {
                    Thread.sleep(backoffTime);
                } catch (InterruptedException e1) {
                    Thread.currentThread().interrupt();
                }
            }
        }
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

}