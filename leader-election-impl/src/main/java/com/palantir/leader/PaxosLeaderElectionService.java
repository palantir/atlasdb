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
package com.palantir.leader;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.palantir.common.remoting.ServiceNotAvailableException;
import com.palantir.logsafe.SafeArg;
import com.palantir.paxos.CoalescingPaxosLatestRoundVerifier;
import com.palantir.paxos.LeaderPinger;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosLatestRoundVerifier;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosLearnerNetworkClient;
import com.palantir.paxos.PaxosProposer;
import com.palantir.paxos.PaxosResponses;
import com.palantir.paxos.PaxosRoundFailureException;
import com.palantir.paxos.PaxosUpdate;
import com.palantir.paxos.PaxosValue;

/**
 * Implementation of a paxos member than can be a designated proposer (leader) and designated
 * learner (informer).
 *
 * @author rullman
 */
public class PaxosLeaderElectionService implements PingableLeader, LeaderElectionService {
    private static final Logger log = LoggerFactory.getLogger(PaxosLeaderElectionService.class);

    private final ReentrantLock lock;
    private final CoalescingPaxosLatestRoundVerifier latestRoundVerifier;

    private final PaxosProposer proposer;
    private final PaxosLearner knowledge;

    private final LeaderPinger leaderPinger;
    private final PingableLeader localPingable;
    private final ImmutableList<PingableLeader> otherPingables;
    private final ImmutableList<PaxosAcceptor> acceptors;
    private final PaxosLearnerNetworkClient learnerClient;

    private final long updatePollingRateInMs;
    private final long randomWaitBeforeProposingLeadership;

    private final PaxosLeaderElectionEventRecorder eventRecorder;

    private final AtomicBoolean leaderEligible = new AtomicBoolean(true);

    PaxosLeaderElectionService(PaxosProposer proposer,
            PaxosLearner knowledge,
            LeaderPinger leaderPinger,
            PingableLeader localPingable,
            List<PingableLeader> otherPingables,
            List<PaxosAcceptor> acceptors,
            PaxosLatestRoundVerifier latestRoundVerifier,
            PaxosLearnerNetworkClient learnerClient,
            long updatePollingWaitInMs,
            long randomWaitBeforeProposingLeadership,
            PaxosLeaderElectionEventRecorder eventRecorder) {
        this.proposer = proposer;
        this.knowledge = knowledge;
        this.leaderPinger = leaderPinger;
        this.localPingable = localPingable;
        this.otherPingables = ImmutableList.copyOf(otherPingables);
        this.acceptors = ImmutableList.copyOf(acceptors);
        this.learnerClient = learnerClient;
        this.updatePollingRateInMs = updatePollingWaitInMs;
        this.randomWaitBeforeProposingLeadership = randomWaitBeforeProposingLeadership;
        lock = new ReentrantLock();
        this.eventRecorder = eventRecorder;
        this.latestRoundVerifier = new CoalescingPaxosLatestRoundVerifier(latestRoundVerifier);
    }

    public void markNotEligibleForLeadership() {
        boolean previousLeaderEligible = leaderEligible.getAndSet(false);
        if (previousLeaderEligible) {
            log.info("Node no longer eligible for leadership");
        }
    }

    @Override
    public LeadershipToken blockOnBecomingLeader() throws InterruptedException {
        while (true) {
            LeadershipState currentState = determineLeadershipState();

            switch (currentState.status()) {
                case LEADING:
                    log.info("Successfully became leader!");
                    return currentState.confirmedToken().get();
                case NO_QUORUM:
                    // If we don't have quorum we should just retry our calls.
                    continue;
                case NOT_LEADING:
                    proposeLeadershipOrWaitForBackoff(currentState);
                    continue;
                default:
                    throw new IllegalStateException("unknown status: " + currentState.status());
            }
        }
    }

    private void proposeLeadershipOrWaitForBackoff(LeadershipState currentState)
            throws InterruptedException {
        if (pingLeader(currentState.greatestLearnedValue())) {
            Thread.sleep(updatePollingRateInMs);
            return;
        }

        boolean learnedNewState = updateLearnedStateFromPeers(currentState.greatestLearnedValue());
        if (learnedNewState) {
            return;
        }

        long backoffTime = (long) (randomWaitBeforeProposingLeadership * Math.random());
        log.debug("Waiting for [{}] ms before proposing leadership", SafeArg.of("waitTimeMs", backoffTime));
        Thread.sleep(backoffTime);

        if (leaderEligible.get()) {
            proposeLeadershipAfter(currentState.greatestLearnedValue());
        } else {
            log.debug("Not eligible for leadership");
        }
    }

    @Override
    public Optional<LeadershipToken> getCurrentTokenIfLeading() {
        return determineLeadershipState().confirmedToken();
    }

    private LeadershipState determineLeadershipState() {
        Optional<PaxosValue> greatestLearnedValue = getGreatestLearnedPaxosValue();
        StillLeadingStatus leadingStatus = determineLeadershipStatus(greatestLearnedValue);

        return LeadershipState.of(greatestLearnedValue, leadingStatus);
    }

    private boolean pingLeader(Optional<PaxosValue> maybeGreatestLearnedValue) {
        return maybeGreatestLearnedValue
                .map(PaxosValue::getLeaderUUID)
                .map(UUID::fromString)
                .map(leaderPinger::pingLeaderWithUuid)
                .orElse(false);
    }

    @Override
    public Set<PingableLeader> getPotentialLeaders() {
        // TODO(jakubk) Not sure about the implications here
        if (leaderEligible.get()) {
            return Sets.union(ImmutableSet.of(localPingable), ImmutableSet.copyOf(otherPingables));
        } else {
            return ImmutableSet.copyOf(otherPingables);
        }
    }

    @Override
    public String getUUID() {
        return proposer.getUuid();
    }

    @Override
    public boolean ping() {
        return localPingable.ping();
    }

    private void proposeLeadershipAfter(Optional<PaxosValue> value) {
        lock.lock();
        try {
            log.debug("Proposing leadership with value [{}]", SafeArg.of("paxosValue", value));
            if (!isLatestRound(value)) {
                // This means that new data has come in so we shouldn't propose leadership.
                // We do this check in a lock to ensure concurrent callers to blockOnBecomingLeader behaves correctly.
                return;
            }

            long seq = getNextSequenceNumber(value);

            eventRecorder.recordProposalAttempt(seq);
            proposer.propose(seq, null);
        } catch (PaxosRoundFailureException e) {
            // We have failed trying to become the leader.
            eventRecorder.recordProposalFailure(e);
        } finally {
            lock.unlock();
        }
    }

    private Optional<PaxosValue> getGreatestLearnedPaxosValue() {
        return Optional.ofNullable(knowledge.getGreatestLearnedValue());
    }

    @Override
    public StillLeadingStatus isStillLeading(LeadershipToken token) {
        if (!(token instanceof PaxosLeadershipToken)) {
            return StillLeadingStatus.NOT_LEADING;
        }

        PaxosLeadershipToken paxosToken = (PaxosLeadershipToken) token;
        return determineAndRecordLeadershipStatus(paxosToken);
    }

    private StillLeadingStatus determineAndRecordLeadershipStatus(
            PaxosLeadershipToken paxosToken) {
        StillLeadingStatus status = determineLeadershipStatus(paxosToken.value);
        recordLeadershipStatus(paxosToken, status);
        return status;
    }

    private StillLeadingStatus determineLeadershipStatus(Optional<PaxosValue> value) {
        return value.map(this::determineLeadershipStatus).orElse(StillLeadingStatus.NOT_LEADING);
    }

    private StillLeadingStatus determineLeadershipStatus(PaxosValue value) {
        if (!isThisNodeTheLeaderFor(value)) {
            return StillLeadingStatus.NOT_LEADING;
        }

        if (!isLatestRound(value)) {
            return StillLeadingStatus.NOT_LEADING;
        }

        return latestRoundVerifier.isLatestRound(value.getRound())
                .toStillLeadingStatus();
    }

    private boolean isLatestRound(PaxosValue value) {
        return isLatestRound(Optional.of(value));
    }

    private boolean isLatestRound(Optional<PaxosValue> valueIfAny) {
        return valueIfAny.equals(getGreatestLearnedPaxosValue());
    }

    private void recordLeadershipStatus(
            PaxosLeadershipToken token,
            StillLeadingStatus status) {
        if (status == StillLeadingStatus.NO_QUORUM) {
            eventRecorder.recordNoQuorum(token.value);
        } else if (status == StillLeadingStatus.NOT_LEADING) {
            eventRecorder.recordNotLeading(token.value);
        }
    }

    private boolean isThisNodeTheLeaderFor(PaxosValue value) {
        return value.getLeaderUUID().equals(proposer.getUuid());
    }

    // This is used by an internal product CLI.
    public ImmutableList<PaxosAcceptor> getAcceptors() {
        return acceptors;
    }

    /**
     * Queries all other learners for unknown learned values.
     *
     * @return true if new state was learned, otherwise false
     */
    private boolean updateLearnedStateFromPeers(Optional<PaxosValue> greatestLearned) {
        final long nextToLearnSeq = getNextSequenceNumber(greatestLearned);

        PaxosResponses<PaxosUpdate> updates = learnerClient.getLearnedValuesSince(nextToLearnSeq);

        // learn the state accumulated from peers
        boolean learned = false;
        for (PaxosUpdate update : updates.get()) {
            ImmutableCollection<PaxosValue> values = update.getValues();
            for (PaxosValue value : values) {
                if (knowledge.getLearnedValue(value.getRound()) == null) {
                    knowledge.learn(value.getRound(), value);
                    learned = true;
                }
            }
        }

        return learned;
    }

    @Override
    public boolean stepDown() {
        LeadershipState leadershipState = determineLeadershipState();
        StillLeadingStatus status = leadershipState.status();
        if (status == StillLeadingStatus.LEADING) {
            try {
                proposer.proposeAnonymously(getNextSequenceNumber(leadershipState.greatestLearnedValue()), null);
                return true;
            } catch (PaxosRoundFailureException e) {
                log.info("Couldn't relinquish leadership because a quorum could not be obtained. Last observed"
                        + " state was {}.",
                        SafeArg.of("leadershipState", leadershipState));
                throw new ServiceNotAvailableException("Couldn't relinquish leadership", e);
            }
        }
        return false;
    }

    private static long getNextSequenceNumber(Optional<PaxosValue> paxosValue) {
        return paxosValue.map(PaxosValue::getRound).orElse(PaxosAcceptor.NO_LOG_ENTRY) + 1;
    }

    @Value.Immutable
    interface LeadershipState {

        @Value.Parameter
        Optional<PaxosValue> greatestLearnedValue();

        @Value.Parameter
        StillLeadingStatus status();

        default Optional<LeadershipToken> confirmedToken() {
            if (status() == StillLeadingStatus.LEADING) {
                return Optional.of(new PaxosLeadershipToken(greatestLearnedValue().get()));
            }
            return Optional.empty();
        }

        static LeadershipState of(Optional<PaxosValue> value, StillLeadingStatus status) {
            return ImmutableLeadershipState.of(value, status);
        }
    }
}
