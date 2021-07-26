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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.ImmutableCollection;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.paxos.LeaderPingResult;
import com.palantir.paxos.LeaderPingResults;
import com.palantir.paxos.LeaderPinger;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosLatestRoundVerifier;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosLearnerNetworkClient;
import com.palantir.paxos.PaxosProposer;
import com.palantir.paxos.PaxosQuorumStatus;
import com.palantir.paxos.PaxosResponses;
import com.palantir.paxos.PaxosRoundFailureException;
import com.palantir.paxos.PaxosUpdate;
import com.palantir.paxos.PaxosValue;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import org.immutables.value.Value;

/**
 * Implementation of a paxos member than can be a designated proposer (leader) and designated
 * learner (informer).
 *
 * @author rullman
 */
public class PaxosLeaderElectionService implements LeaderElectionService {
    private static final SafeLogger log = SafeLoggerFactory.get(PaxosLeaderElectionService.class);

    // stored here to be consistent when proposing to take over leadership
    private static final byte[] LEADERSHIP_PROPOSAL_VALUE = null;

    private final ReentrantLock lock = new ReentrantLock();
    private final PaxosLatestRoundVerifier latestRoundVerifier;

    private final PaxosProposer proposer;
    private final PaxosLearner knowledge;

    private final LeaderPinger leaderPinger;
    private final PaxosLearnerNetworkClient learnerClient;

    private final Duration updatePollingRate;
    private final Duration randomWaitBeforeProposingLeadership;

    private final PaxosLeaderElectionEventRecorder eventRecorder;

    private final AtomicBoolean leaderEligible = new AtomicBoolean(true);
    private final RateLimiter leaderEligibilityLoggingRateLimiter = RateLimiter.create(1);

    private final Cache<UUID, HostAndPort> leaderAddressCache;

    PaxosLeaderElectionService(
            PaxosProposer proposer,
            PaxosLearner knowledge,
            LeaderPinger leaderPinger,
            PaxosLatestRoundVerifier latestRoundVerifier,
            PaxosLearnerNetworkClient learnerClient,
            Duration updatePollingWait,
            Duration randomWaitBeforeProposingLeadership,
            Duration leaderAddressCacheTtl,
            PaxosLeaderElectionEventRecorder eventRecorder) {
        this.proposer = proposer;
        this.knowledge = knowledge;
        this.leaderPinger = leaderPinger;
        this.latestRoundVerifier = latestRoundVerifier;
        this.learnerClient = learnerClient;
        this.updatePollingRate = updatePollingWait;
        this.randomWaitBeforeProposingLeadership = randomWaitBeforeProposingLeadership;
        this.eventRecorder = eventRecorder;
        this.leaderAddressCache =
                Caffeine.newBuilder().expireAfterWrite(leaderAddressCacheTtl).build();
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
            throw new InterruptedException("leader no longer eligible");
        }

        if (pingLeader(currentState.greatestLearnedValue()).isSuccessful()) {
            Thread.sleep(updatePollingRate.toMillis());
            return;
        }

        boolean learnedNewState = updateLearnedStateFromPeers(currentState.greatestLearnedValue());
        if (learnedNewState) {
            return;
        }

        long backoffTime = (long) (randomWaitBeforeProposingLeadership.toMillis() * Math.random());
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
        Optional<LeaderPingResult> maybeLeaderPingResult =
                extractLeaderUuid(maybeGreatestLearnedValue).map(leaderPinger::pingLeaderWithUuid);
        maybeLeaderPingResult.ifPresent(leaderPingResult -> leaderPingResult.recordEvent(eventRecorder));
        maybeLeaderPingResult.ifPresent(leaderPingResult -> LeaderPingResults.caseOf(leaderPingResult)
                .pingReturnedTrue((key, value) -> {
                    leaderAddressCache.put(key, value);
                    return null;
                })
                .otherwise_(null));
        return maybeLeaderPingResult.orElseGet(LeaderPingResults::pingReturnedFalse);
    }

    private static Optional<UUID> extractLeaderUuid(Optional<PaxosValue> maybeGreatestLearnedValue) {
        return maybeGreatestLearnedValue.map(PaxosValue::getLeaderUUID).map(UUID::fromString);
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
            proposer.propose(seq, LEADERSHIP_PROPOSAL_VALUE);
        } catch (PaxosRoundFailureException e) {
            // We have failed trying to become the leader.
            eventRecorder.recordProposalFailure(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public ListenableFuture<StillLeadingStatus> isStillLeading(LeadershipToken token) {
        if (!(token instanceof PaxosLeadershipToken)) {
            return Futures.immediateFuture(StillLeadingStatus.NOT_LEADING);
        }
        PaxosLeadershipToken paxosToken = (PaxosLeadershipToken) token;
        return determineAndRecordLeadershipStatus(paxosToken);
    }

    private ListenableFuture<StillLeadingStatus> determineAndRecordLeadershipStatus(PaxosLeadershipToken paxosToken) {
        ListenableFuture<StillLeadingStatus> statusFuture = determineLeadershipStatus(paxosToken.value);
        return Futures.transform(
                statusFuture,
                status -> {
                    recordLeadershipStatus(paxosToken, status);
                    return status;
                },
                MoreExecutors.directExecutor());
    }

    private StillLeadingStatus determineLeadershipStatus(Optional<PaxosValue> value) {
        return value.map(this::determineLeadershipStatus)
                .map(Futures::getUnchecked)
                .orElse(StillLeadingStatus.NOT_LEADING);
    }

    private ListenableFuture<StillLeadingStatus> determineLeadershipStatus(PaxosValue value) {
        if (!isThisNodeTheLeaderFor(value)) {
            return Futures.immediateFuture(StillLeadingStatus.NOT_LEADING);
        }

        if (!isLatestRound(value)) {
            return Futures.immediateFuture(StillLeadingStatus.NOT_LEADING);
        }

        return Futures.transform(
                latestRoundVerifier.isLatestRoundAsync(value.getRound()),
                PaxosQuorumStatus::toStillLeadingStatus,
                MoreExecutors.directExecutor());
    }

    private boolean isLatestRound(PaxosValue value) {
        return isLatestRound(Optional.of(value));
    }

    private boolean isLatestRound(Optional<PaxosValue> valueIfAny) {
        return valueIfAny.equals(knowledge.getGreatestLearnedValue());
    }

    private void recordLeadershipStatus(PaxosLeadershipToken token, StillLeadingStatus status) {
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
                proposer.proposeAnonymously(
                        getNextSequenceNumber(leadershipState.greatestLearnedValue()), LEADERSHIP_PROPOSAL_VALUE);
                return true;
            } catch (PaxosRoundFailureException e) {
                log.info(
                        "Couldn't relinquish leadership because a quorum could not be obtained. Last observed"
                                + " state was {}.",
                        SafeArg.of("leadershipState", leadershipState),
                        e);
                return false;
            }
        }
        return false;
    }

    @Override
    public boolean hostileTakeover() {
        LeadershipState leadershipState = determineLeadershipState();
        StillLeadingStatus status = leadershipState.status();
        switch (status) {
            case LEADING:
                return true;
            case NOT_LEADING:
                try {
                    proposer.propose(
                            getNextSequenceNumber(leadershipState.greatestLearnedValue()), LEADERSHIP_PROPOSAL_VALUE);
                    StillLeadingStatus newStatus = determineLeadershipState().status();
                    if (newStatus == StillLeadingStatus.LEADING) {
                        log.info("Successfully took over", SafeArg.of("newStatus", newStatus));
                        return true;
                    } else {
                        log.info("Failed to take over", SafeArg.of("newStatus", newStatus));
                        return false;
                    }
                } catch (PaxosRoundFailureException e) {
                    log.info(
                            "Couldn't takeover leadership because a quorum could not be obtained.",
                            SafeArg.of("lastObservedState", leadershipState),
                            e);
                    return false;
                }
            case NO_QUORUM:
                log.info("Couldn't takeover leadership because a quorum could not be obtained: NO_QUORUM");
                return false;
            default:
                throw new IllegalStateException("Unexpected value: " + status);
        }
    }

    @Override
    public Optional<HostAndPort> getRecentlyPingedLeaderHost() {
        return extractLeaderUuid(knowledge.getGreatestLearnedValue()).map(leaderAddressCache::getIfPresent);
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
                return Optional.of(
                        new PaxosLeadershipToken(greatestLearnedValue().get()));
            }
            return Optional.empty();
        }

        static LeadershipState of(Optional<PaxosValue> value, StillLeadingStatus status) {
            return ImmutableLeadershipState.of(value, status);
        }
    }
}
