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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.Nullable;

import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.RateLimiter;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.MultiplexingCompletionService;
import com.palantir.logsafe.SafeArg;
import com.palantir.paxos.CoalescingPaxosLatestRoundVerifier;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosLatestRoundVerifierImpl;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosProposer;
import com.palantir.paxos.PaxosQuorumChecker;
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
    private static final double LOG_RATE = 1;

    private final RateLimiter logRateLimiter = RateLimiter.create(LOG_RATE);
    private final ReentrantLock lock;
    private final CoalescingPaxosLatestRoundVerifier latestRoundVerifier;

    final PaxosProposer proposer;
    final PaxosLearner knowledge;

    final Map<PingableLeader, HostAndPort> otherPotentialLeadersToHosts;
    final ImmutableList<PaxosAcceptor> acceptors;
    final ImmutableList<PaxosLearner> learners;

    final long updatePollingRateInMs;
    final long randomWaitBeforeProposingLeadership;
    final long leaderPingResponseWaitMs;

    final Map<PingableLeader, ExecutorService> leaderPingExecutors;
    final Map<PaxosLearner, ExecutorService> knowledgeUpdatingExecutors;

    final ConcurrentMap<String, PingableLeader> uuidToServiceCache = Maps.newConcurrentMap();

    private final PaxosLeaderElectionEventRecorder eventRecorder;

    /**
     * @deprecated Use PaxosLeaderElectionServiceBuilder instead.
     */
    @Deprecated
    public PaxosLeaderElectionService(PaxosProposer proposer,
            PaxosLearner knowledge,
            Map<PingableLeader, HostAndPort> otherPotentialLeadersToHosts,
            List<PaxosAcceptor> acceptors,
            List<PaxosLearner> learners,
            Function<String, ExecutorService> executorServiceFactory,
            long updatePollingWaitInMs,
            long randomWaitBeforeProposingLeadership,
            long leaderPingResponseWaitMs,
            PaxosLeaderElectionEventRecorder eventRecorder) {
        this.proposer = proposer;
        this.knowledge = knowledge;
        // XXX This map uses something that may be proxied as a key! Be very careful if making a new map from this.
        this.otherPotentialLeadersToHosts = Collections.unmodifiableMap(otherPotentialLeadersToHosts);
        this.acceptors = ImmutableList.copyOf(acceptors);
        this.learners = ImmutableList.copyOf(learners);
        this.leaderPingExecutors = createLeaderPingExecutors(otherPotentialLeadersToHosts, executorServiceFactory);
        this.knowledgeUpdatingExecutors = createKnowledgeUpdateExecutors(learners, executorServiceFactory);
        this.updatePollingRateInMs = updatePollingWaitInMs;
        this.randomWaitBeforeProposingLeadership = randomWaitBeforeProposingLeadership;
        this.leaderPingResponseWaitMs = leaderPingResponseWaitMs;
        lock = new ReentrantLock();
        this.eventRecorder = eventRecorder;
        this.latestRoundVerifier = new CoalescingPaxosLatestRoundVerifier(
                new PaxosLatestRoundVerifierImpl(
                        acceptors,
                        proposer.getQuorumSize(),
                        createLatestRoundVerifierExecutors(acceptors, executorServiceFactory)));
    }

    private Map<PingableLeader, ExecutorService> createLeaderPingExecutors(
            Map<PingableLeader, HostAndPort> otherLeadersToHosts,
            Function<String, ExecutorService> executorServiceFactory) {
        Map<PingableLeader, ExecutorService> executors = Maps.newHashMap();
        executors.put(this, executorServiceFactory.apply("leader-ping-0"));

        int index = 1;
        for (PingableLeader leader : otherLeadersToHosts.keySet()) {
            executors.put(leader, executorServiceFactory.apply("leader-ping-" + index));
            index++;
        }

        return executors;
    }

    private Map<PaxosLearner, ExecutorService> createKnowledgeUpdateExecutors(
            List<PaxosLearner> paxosLearners,
            Function<String, ExecutorService> executorServiceFactory) {
        return IntStream.range(0, paxosLearners.size())
                .boxed()
                .collect(Collectors.toMap(
                        paxosLearners::get,
                        index -> executorServiceFactory.apply("knowledge-update-" + index)));
    }

    private Map<PaxosAcceptor, ExecutorService> createLatestRoundVerifierExecutors(
            List<PaxosAcceptor> paxosAcceptors,
            Function<String, ExecutorService> executorServiceFactory) {
        Map<PaxosAcceptor, ExecutorService> executors = Maps.newHashMap();
        for (int i = 0; i < paxosAcceptors.size(); i++) {
            executors.put(paxosAcceptors.get(i), executorServiceFactory.apply("latest-round-verifier-" + i));
        }
        return executors;
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
        if (pingLeader()) {
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
        Optional<PaxosValue> greatestLearnedValue = getGreatestLearnedPaxosValue();
        StillLeadingStatus leadingStatus = determineLeadershipStatus(greatestLearnedValue);

        return LeadershipState.of(greatestLearnedValue, leadingStatus);
    }

    private boolean pingLeader() {
        Optional<PingableLeader> maybeLeader = getSuspectedLeader(true /* use network */);
        if (!maybeLeader.isPresent()) {
            return false;
        }
        final PingableLeader leader = maybeLeader.get();

        MultiplexingCompletionService<PingableLeader, Boolean> multiplexingCompletionService
                = MultiplexingCompletionService.create(leaderPingExecutors);

        multiplexingCompletionService.submit(leader, leader::ping);

        try {
            Future<Boolean> pingFuture = multiplexingCompletionService.poll(
                    leaderPingResponseWaitMs,
                    TimeUnit.MILLISECONDS);
            return getAndRecordLeaderPingResult(pingFuture);
        } catch (InterruptedException ex) {
            return false;
        }
    }

    @VisibleForTesting
    boolean getAndRecordLeaderPingResult(@Nullable Future<Boolean> pingFuture) throws InterruptedException {
        if (pingFuture == null) {
            eventRecorder.recordLeaderPingTimeout();
            return false;
        }

        try {
            boolean isLeader = pingFuture.get();
            if (!isLeader) {
                eventRecorder.recordLeaderPingReturnedFalse();
            }
            return isLeader;
        } catch (ExecutionException ex) {
            eventRecorder.recordLeaderPingFailure(ex.getCause());
            return false;
        }
    }

    @Override
    public Optional<HostAndPort> getSuspectedLeaderInMemory() {
        Optional<PingableLeader> maybeLeader = getSuspectedLeader(false /* use network */);
        if (!maybeLeader.isPresent()) {
            return Optional.empty();
        }
        return Optional.of(otherPotentialLeadersToHosts.get(maybeLeader.get()));
    }

    private Optional<PingableLeader> getSuspectedLeader(boolean useNetwork) {
        PaxosValue value = knowledge.getGreatestLearnedValue();
        if (value == null) {
            return Optional.empty();
        }

        // check leader cache
        String uuid = value.getLeaderUUID();
        if (uuidToServiceCache.containsKey(uuid)) {
            return Optional.of(uuidToServiceCache.get(uuid));
        }

        if (useNetwork) {
            return getSuspectedLeaderOverNetwork(uuid);
        } else {
            return Optional.empty();
        }
    }

    private Optional<PingableLeader> getSuspectedLeaderOverNetwork(String uuid) {
        MultiplexingCompletionService<PingableLeader, Entry<String, PingableLeader>> pingService
                = MultiplexingCompletionService.create(leaderPingExecutors);

        // kick off requests to get leader uuids
        List<Future<Entry<String, PingableLeader>>> allFutures = Lists.newArrayList();
        for (final PingableLeader potentialLeader : otherPotentialLeadersToHosts.keySet()) {
            try {
                allFutures.add(pingService.submit(potentialLeader,
                        () -> Maps.immutableEntry(potentialLeader.getUUID(), potentialLeader)));
            } catch (RejectedExecutionException e) {
                if (logRateLimiter.tryAcquire()) {
                    log.info("Leader ping executor rejected task. "
                            + "This may signal a problem with cluster if happens frequently");
                }
            }
        }

        // collect responses
        boolean interrupted = false;
        try {
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(leaderPingResponseWaitMs);
            for (int i = 0; i < allFutures.size(); i++) {
                try {
                    Future<Entry<String, PingableLeader>> pingFuture = pingService.poll(
                            deadline - System.nanoTime(),
                            TimeUnit.NANOSECONDS);
                    if (pingFuture == null) {
                        break;
                    }

                    // cache remote leader uuid
                    Entry<String, PingableLeader> cacheEntry = pingFuture.get();
                    PingableLeader service = uuidToServiceCache.putIfAbsent(cacheEntry.getKey(), cacheEntry.getValue());
                    throwIfInvalidSetup(service, cacheEntry.getValue(), cacheEntry.getKey());

                    // return the leader if it matches
                    if (uuid.equals(cacheEntry.getKey())) {
                        return Optional.of(cacheEntry.getValue());
                    }
                } catch (InterruptedException e) {
                    log.warn("uuid request interrupted", e);
                    interrupted = true;
                    break;
                } catch (ExecutionException e) {
                    log.warn("unable to get uuid from server", e);
                }
            }
        } finally {
            // cancel pending futures
            for (Future<Entry<String, PingableLeader>> future : allFutures) {
                future.cancel(false);
            }

            // reset interrupted flag
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }

        return Optional.empty();
    }

    private void throwIfInvalidSetup(PingableLeader cachedService,
                                     PingableLeader pingedService,
                                     String pingedServiceUuid) {
        if (cachedService == null) {
            return;
        }

        IllegalStateException exception = new IllegalStateException(
                "There is a fatal problem with the leadership election configuration! "
                        + "This is probably caused by invalid pref files setting up the cluster "
                        + "(e.g. for lock server look at lock.prefs, leader.prefs, and lock_client.prefs)."
                        + "If the preferences are specified with a host port pair list and localhost index "
                        + "then make sure that the localhost index is correct (e.g. actually the localhost).");

        if (cachedService != pingedService) {
            log.error("Remote potential leaders are claiming to be each other!", exception);
            throw Throwables.rewrap(exception);
        }

        if (pingedServiceUuid.equals(getUUID())) {
            log.error("Remote potential leader is claiming to be you!", exception);
            throw Throwables.rewrap(exception);
        }
    }

    @Override
    public Set<PingableLeader> getPotentialLeaders() {
        return Sets.union(ImmutableSet.of(this), otherPotentialLeadersToHosts.keySet());
    }

    @Override
    public String getUUID() {
        return proposer.getUuid();
    }

    @Override
    public boolean ping() {
        return getGreatestLearnedPaxosValue()
                .map(this::isThisNodeTheLeaderFor)
                .orElse(false);
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

            long seq = value.map(PaxosValue::getRound).orElse(PaxosAcceptor.NO_LOG_ENTRY) + 1;

            eventRecorder.recordProposalAttempt(seq);
            proposer.propose(seq, null);
        } catch (PaxosRoundFailureException e) {
            // We have failed trying to become the leader.
            eventRecorder.recordProposalFailure(e);
            return;
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
     * @returns true if new state was learned, otherwise false
     */
    public boolean updateLearnedStateFromPeers(Optional<PaxosValue> greatestLearned) {
        final long nextToLearnSeq = greatestLearned.map(PaxosValue::getRound).orElse(PaxosAcceptor.NO_LOG_ENTRY) + 1;
        PaxosResponses<PaxosUpdate> updates = PaxosQuorumChecker.collectQuorumResponses(
                learners,
                learner -> new PaxosUpdate(ImmutableList.copyOf(learner.getLearnedValuesSince(nextToLearnSeq))),
                proposer.getQuorumSize(),
                knowledgeUpdatingExecutors,
                PaxosQuorumChecker.DEFAULT_REMOTE_REQUESTS_TIMEOUT);

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
