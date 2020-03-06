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

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;
import com.palantir.logsafe.SafeArg;
import com.palantir.paxos.LeaderPingResult;
import com.palantir.paxos.LeaderPingResults;
import com.palantir.paxos.LeaderPinger;

/**
 * Implementation of a paxos member than can be a designated proposer (leader) and designated
 * learner (informer).
 *
 * @author rullman
 */
public class PaxosLeaderElectionService implements LeaderElectionService {
    private static final Logger log = LoggerFactory.getLogger(PaxosLeaderElectionService.class);

    private final ReentrantLock lock = new ReentrantLock();

    private final LeaderPinger leaderPinger;

    private final Duration updatePollingRate;
    private final Duration randomWaitBeforeProposingLeadership;

    private final PaxosLeaderElectionEventRecorder eventRecorder;

    private final AtomicBoolean leaderEligible = new AtomicBoolean(true);
    private final RateLimiter leaderEligibilityLoggingRateLimiter = RateLimiter.create(1);

    private final Cache<UUID, HostAndPort> leaderAddressCache;

    private final AtomicValue<Leadership> leadership = null;
    private final UUID ourId = null;

    PaxosLeaderElectionService(
            LeaderPinger leaderPinger,
            Duration updatePollingWait,
            Duration randomWaitBeforeProposingLeadership,
            Duration leaderAddressCacheTtl,
            PaxosLeaderElectionEventRecorder eventRecorder) {
        this.leaderPinger = leaderPinger;
        this.updatePollingRate = updatePollingWait;
        this.randomWaitBeforeProposingLeadership = randomWaitBeforeProposingLeadership;
        this.eventRecorder = eventRecorder;
        this.leaderAddressCache = Caffeine.newBuilder()
                .expireAfterWrite(leaderAddressCacheTtl)
                .build();
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

        if (pingLeader(currentState.getLeadership()).isSuccessful()) {
            Thread.sleep(updatePollingRate.toMillis());
            return;
        }

        long backoffTime = (long) (randomWaitBeforeProposingLeadership.toMillis() * Math.random());
        log.debug("Waiting for [{}] ms before proposing leadership", SafeArg.of("waitTimeMs", backoffTime));
        Thread.sleep(backoffTime);

        proposeLeadershipAfter(currentState.getLeadership());
    }

    @Override
    public Optional<LeadershipToken> getCurrentTokenIfLeading() {
        return determineLeadershipState().confirmedToken();
    }

    private LeadershipState determineLeadershipState() {
        Optional<Leadership> leader = leadership.get();
        StillLeadingStatus leadingStatus = determineLeadershipStatus(leader);

        return LeadershipState.of(leader, leadingStatus);
    }

    private LeaderPingResult pingLeader(Optional<Leadership> maybeGreatestLearnedValue) {
        Optional<LeaderPingResult> maybeLeaderPingResult = extractLeaderUuid(maybeGreatestLearnedValue)
                .map(leaderPinger::pingLeaderWithUuid);
        maybeLeaderPingResult.ifPresent(leaderPingResult -> leaderPingResult.recordEvent(eventRecorder));
        maybeLeaderPingResult.ifPresent(leaderPingResult -> LeaderPingResults.caseOf(leaderPingResult)
                .pingReturnedTrue((key, value) -> {
                    leaderAddressCache.put(key, value);
                    return null;
                }).otherwise_(null));
        return maybeLeaderPingResult.orElseGet(LeaderPingResults::pingReturnedFalse);
    }

    private static Optional<UUID> extractLeaderUuid(Optional<Leadership> maybeGreatestLearnedValue) {
        return maybeGreatestLearnedValue.map(Leadership::leaderId);
    }

    private void proposeLeadershipAfter(Optional<Leadership> value) {
        lock.lock();
        try {
            log.debug("Proposing leadership with value [{}]", SafeArg.of("paxosValue", value));
            leadership.compareAndSet(value, Optional.of(Leadership.newProposal(ourId)));
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

    private ListenableFuture<StillLeadingStatus> determineAndRecordLeadershipStatus(
            PaxosLeadershipToken paxosToken) {
        ListenableFuture<StillLeadingStatus> statusFuture = determineLeadershipStatus(paxosToken.leadership());
        return Futures.transform(statusFuture, status -> {
            recordLeadershipStatus(paxosToken, status);
            return status;
        }, MoreExecutors.directExecutor());
    }

    private StillLeadingStatus determineLeadershipStatus(Optional<Leadership> value) {
        return value.map(this::determineLeadershipStatus)
                .map(Futures::getUnchecked)
                .orElse(StillLeadingStatus.NOT_LEADING);
    }

    private ListenableFuture<StillLeadingStatus> determineLeadershipStatus(Leadership value) {
        if (!isThisNodeTheLeaderFor(value)) {
            return Futures.immediateFuture(StillLeadingStatus.NOT_LEADING);
        }

        if (!isLatestRound(value)) {
            return Futures.immediateFuture(StillLeadingStatus.NOT_LEADING);
        }

        return Futures.immediateFuture(StillLeadingStatus.LEADING);
    }

    private boolean isLatestRound(Leadership value) {
        return isLatestRound(Optional.of(value));
    }

    private boolean isLatestRound(Optional<Leadership> valueIfAny) {
        return leadership.get().equals(valueIfAny);
    }

    private void recordLeadershipStatus(
            PaxosLeadershipToken token,
            StillLeadingStatus status) {
        if (status == StillLeadingStatus.NO_QUORUM) {
            eventRecorder.recordNoQuorum(token.leadership());
        } else if (status == StillLeadingStatus.NOT_LEADING) {
            eventRecorder.recordNotLeading(token.leadership());
        }
    }

    private boolean isThisNodeTheLeaderFor(Leadership value) {
        return value.leaderId().equals(ourId);
    }

    @Override
    public boolean stepDown() {
        LeadershipState leadershipState = determineLeadershipState();
        StillLeadingStatus status = leadershipState.status();
        if (status == StillLeadingStatus.LEADING) {
            return leadership.compareAndSet(leadershipState.getLeadership(), Optional.empty());
        }
        return false;
    }

    @Override
    public Optional<HostAndPort> getRecentlyPingedLeaderHost() {
        return leadership.getStale().map(Leadership::leaderId).map(leaderAddressCache::getIfPresent);
    }

    @Value.Immutable
    interface LeadershipState {

        @Value.Parameter
        Optional<Leadership> getLeadership();

        @Value.Parameter
        StillLeadingStatus status();

        default Optional<LeadershipToken> confirmedToken() {
            if (status() == StillLeadingStatus.LEADING) {
                return Optional.of(PaxosLeadershipToken.of(getLeadership().get()));
            }
            return Optional.empty();
        }

        static LeadershipState of(Optional<Leadership> value, StillLeadingStatus status) {
            return ImmutableLeadershipState.of(value, status);
        }
    }
}
