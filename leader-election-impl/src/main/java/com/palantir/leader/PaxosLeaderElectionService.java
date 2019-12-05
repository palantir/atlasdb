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

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableCollection;
import com.google.common.util.concurrent.RateLimiter;
import com.palantir.common.remoting.ServiceNotAvailableException;
import com.palantir.logsafe.SafeArg;
import com.palantir.paxos.CoalescingPaxosLatestRoundVerifier;
import com.palantir.paxos.LeaderPingResult;
import com.palantir.paxos.LeaderPingResults;
import com.palantir.paxos.LeaderPinger;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosAcceptorNetworkClient;
import com.palantir.paxos.PaxosLatestRoundVerifierImpl;
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
public class PaxosLeaderElectionService implements LeaderElectionService {
    private static final Logger log = LoggerFactory.getLogger(PaxosLeaderElectionService.class);

    private final ReentrantLock lock = new ReentrantLock();
    private final CoalescingPaxosLatestRoundVerifier latestRoundVerifier;

    private final PaxosProposer proposer;
    private final PaxosLearner knowledge;

    private final LeaderPinger leaderPinger;
    private final PaxosLearnerNetworkClient learnerClient;

    private final long updatePollingRateInMs;
    private final long randomWaitBeforeProposingLeadership;

    private final PaxosLeaderElectionEventRecorder eventRecorder;

    private final AtomicBoolean leaderEligible = new AtomicBoolean(true);
    private final RateLimiter leaderEligibilityLoggingRateLimiter = RateLimiter.create(1);

    PaxosLeaderElectionService(
            PaxosProposer proposer,
            PaxosLearner knowledge,
            LeaderPinger leaderPinger,
            PaxosAcceptorNetworkClient acceptorClient,
            PaxosLearnerNetworkClient learnerClient,
            long updatePollingWaitInMs,
            long randomWaitBeforeProposingLeadership,
            PaxosLeaderElectionEventRecorder eventRecorder) {
        this.proposer = proposer;
        this.knowledge = knowledge;
        this.leaderPinger = leaderPinger;
        this.learnerClient = learnerClient;
        this.updatePollingRateInMs = updatePollingWaitInMs;
        this.randomWaitBeforeProposingLeadership = randomWaitBeforeProposingLeadership;
        this.eventRecorder = eventRecorder;
        this.latestRoundVerifier =
                new CoalescingPaxosLatestRoundVerifier(new PaxosLatestRoundVerifierImpl(acceptorClient));
    }

    @Override
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

    private void proposeLeadershipOrWaitForBackoff(LeadershipState currentState) throws InterruptedException {
        if (!leaderEligible.get()) {
            if (leaderEligibilityLoggingRateLimiter.tryAcquire()) {
                log.debug("Not eligible for leadership");
            }
            return;
        }

        if (pingLeader(currentState.greatestLearnedValue()).isSuccessful()) {
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

        proposeLeadershipAfter(currentState.greatestLearnedValue());
    }

    @Override
    public Optional<LeadershipToken> getCurrentTokenIfLeading() {
        return determineLeadershipState().confirmedToken();
    }

    private LeadershipState determineLeadershipState() {
        Optional<PaxosValue> greatestLearnedValue = knowledge.getGreatestLearnedValue();
        StillLeadingStatus leadingStatus = determineLeadershipStatus(greatestLearnedValue);

        return LeadershipState.of(greatestLearnedValue, leadingStatus);
    }

    private LeaderPingResult pingLeader(Optional<PaxosValue> maybeGreatestLearnedValue) {
        Optional<LeaderPingResult> maybeLeaderPingResult = maybeGreatestLearnedValue
                .map(PaxosValue::getLeaderUUID)
                .map(UUID::fromString)
                .map(leaderPinger::pingLeaderWithUuid);
        maybeLeaderPingResult.ifPresent(leaderPingResult -> leaderPingResult.recordEvent(eventRecorder));
        return maybeLeaderPingResult.orElseGet(LeaderPingResults::pingReturnedFalse);
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
        return valueIfAny.equals(knowledge.getGreatestLearnedValue());
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
                if (!knowledge.getLearnedValue(value.getRound()).isPresent()) {
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
