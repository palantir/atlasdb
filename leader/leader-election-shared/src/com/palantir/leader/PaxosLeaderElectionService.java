// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.leader;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Defaults;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.palantir.common.base.Throwables;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosProposer;
import com.palantir.paxos.PaxosQuorumChecker;
import com.palantir.paxos.PaxosResponse;
import com.palantir.paxos.BooleanPaxosResponse;
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

    public static final long DEFAULT_UPDATE_POLLING_WAIT_IN_MS = 1000;
    public static final long DEFAULT_RANDOM_WAIT_BEFORE_PROPOSING_LEADERSHIP_IN_MS = 2000;
    public static final long DEFAULT_LEADER_PING_RESPONSE_WAIT_IN_MS = 2000;

    private final ReentrantLock lock;

    final PaxosProposer proposer;
    final PaxosLearner knowledge;

    final List<PingableLeader> potentialLeaders;
    final ImmutableList<PaxosAcceptor> acceptors;
    final ImmutableList<PaxosLearner> learners;

    final long updatePollingRateInMs;
    final long randomWaitBeforeProposingLeadership;
    final long leaderPingResponseWaitMs;

    final Executor executor;

    final ConcurrentMap<String, PingableLeader> uuidToServiceCache = Maps.newConcurrentMap();

    public PaxosLeaderElectionService(PaxosProposer proposer,
                                      PaxosLearner knowledge,
                                      List<PingableLeader> potentialLeaders,
                                      ImmutableList<PaxosAcceptor> acceptors,
                                      ImmutableList<PaxosLearner> learners,
                                      Executor executor,
                                      long updatePollingWaitInMs,
                                      long randomWaitBeforeProposingLeadership,
                                      long leaderPingResponseWaitMs) {
        this.proposer = proposer;
        this.knowledge = knowledge;
        this.potentialLeaders = Lists.newArrayList(potentialLeaders);
        this.acceptors = acceptors;
        this.learners = learners;
        this.executor = executor;
        this.updatePollingRateInMs = updatePollingWaitInMs;
        this.randomWaitBeforeProposingLeadership = randomWaitBeforeProposingLeadership;
        this.leaderPingResponseWaitMs = leaderPingResponseWaitMs;
        lock = new ReentrantLock();
    }

    @VisibleForTesting
    void addPeer(PingableLeader peer) {
        for (PingableLeader leader : potentialLeaders) {
            if (leader.equals(peer)) {
                return;
            }
        }
        potentialLeaders.add(peer);
    }

    @VisibleForTesting
    void removePeer(PingableLeader peer) {
        for (PingableLeader leader : potentialLeaders) {
            if (leader.equals(peer)) {
                potentialLeaders.remove(peer);
                return;
            }
        }
    }

    @Override
    public LeadershipToken blockOnBecomingLeader() throws InterruptedException {
        for (;;) {
            PaxosValue greatestLearned = knowledge.getGreatestLearnedValue();
            LeadershipToken token = genTokenFromValue(greatestLearned);

            if (isLastConfirmedLeader(greatestLearned)) {
                StillLeadingStatus leadingStatus = isStillLeading(token);
                if (leadingStatus == StillLeadingStatus.LEADING) {
                    return token;
                } else if (leadingStatus == StillLeadingStatus.NO_QUORUM) {
                    // If we don't have quorum we should just retry our calls.
                    continue;
                }
            } else {
                // We are not the leader, so we should ping them to see if they are still up.
                if (pingLeader()) {
                    Thread.sleep(updatePollingRateInMs);
                    continue;
                }
            }

            boolean learnedNewState = updateLearnedStateFromPeers(greatestLearned);
            if (learnedNewState) {
                continue;
            }

            long backoffTime = (long) (randomWaitBeforeProposingLeadership * Math.random());
            Thread.sleep(backoffTime);

            proposeLeadership(token);
        }
    }

    private LeadershipToken genTokenFromValue(PaxosValue value) {
        return new PaxosLeadershipToken(value);
    }

    private boolean pingLeader() {
        final PingableLeader leader = getSuspectedLeader();
        if (leader == null) {
            return false;
        }

        CompletionService<Boolean> pingCompletionService = new ExecutorCompletionService<Boolean>(
                executor);

        // kick off all the requests
        pingCompletionService.submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return leader.ping();
            }
        });

        try {
            Future<Boolean> pingFuture = pingCompletionService.poll(
                    leaderPingResponseWaitMs,
                    TimeUnit.MILLISECONDS);
            return pingFuture != null && pingFuture.get();
        } catch (InterruptedException e) {
            return false;
        } catch (ExecutionException e) {
            log.warn("cannot ping leader", e);
            return false;
        }
    }

    private PingableLeader getSuspectedLeader() {
        PaxosValue value = knowledge.getGreatestLearnedValue();
        if (value == null) {
            return null;
        }

        // check leader cache
        String uuid = value.getLeaderUUID();
        if (uuidToServiceCache.containsKey(uuid)) {
            return uuidToServiceCache.get(uuid);
        }

        return getSuspectedLeaderOverNetwork(uuid);
    }

    private PingableLeader getSuspectedLeaderOverNetwork(String uuid) {
        CompletionService<Entry<String, PingableLeader>> pingService = new ExecutorCompletionService<Entry<String, PingableLeader>>(
                executor);

        // kick off requests to get leader uuids
        List<Future<Entry<String, PingableLeader>>> allFutures = Lists.newArrayList();
        for (final PingableLeader potentialLeader : potentialLeaders) {
            allFutures.add(pingService.submit(new Callable<Entry<String, PingableLeader>>() {
                @Override
                public Entry<String, PingableLeader> call() throws Exception {
                    return new AbstractMap.SimpleEntry<String, PingableLeader>(
                            potentialLeader.getUUID(),
                            potentialLeader);
                }
            }));
        }

        // collect responses
        boolean interrupted = false;
        try {
            long deadline = System.nanoTime()
                    + TimeUnit.MILLISECONDS.toNanos(leaderPingResponseWaitMs);
            for (;;) {
                try {
                    Future<Entry<String, PingableLeader>> pingFuture = pingService.poll(
                            deadline - System.nanoTime(),
                            TimeUnit.NANOSECONDS);
                    if (pingFuture == null) {
                        break;
                    }

                    // cache remote leader uuid
                    Entry<String, PingableLeader> cacheEntry = pingFuture.get();
                    PingableLeader service = uuidToServiceCache.putIfAbsent(
                            cacheEntry.getKey(),
                            cacheEntry.getValue());
                    throwIfInvalidSetup(service, cacheEntry.getValue(), cacheEntry.getKey());

                    // return the leader if it matches
                    if (uuid.equals(cacheEntry.getKey())) {
                        return cacheEntry.getValue();
                    }
                } catch (InterruptedException e) {
                    log.warn("uuid request interrupted", e);
                    interrupted = true;
                    break;
                } catch (ExecutionException e) {
                    log.warn("unable to get uuid from server", e);
                }
            }

            // poll for extra completed futures
            Future<Entry<String, PingableLeader>> future;
            while ((future = pingService.poll()) != null) {
                try {
                    Entry<String, PingableLeader> cacheEntry = future.get();
                    uuidToServiceCache.putIfAbsent(cacheEntry.getKey(), cacheEntry.getValue());
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

        return null;
    }

    private void throwIfInvalidSetup(PingableLeader cachedService,
                                     PingableLeader pingedService,
                                     String pingedServiceUuid) {
        if (cachedService == null) {
            return;
        }

        IllegalStateException e = new IllegalStateException(
                "There is a fatal problem with the leadership election configuration! "
              + "This is probably caused by invalid pref files setting up the cluster "
              + "(e.g. for lock server look at lock.prefs, leader.prefs, and lock_client.prefs)."
              + "If the preferences are specified with a host port pair list and localhost index "
              + "then make sure that the localhost index is correct (e.g. actually the localhost).");

        if (cachedService != pingedService) {
            log.error("Remote potential leaders are claiming to be each other!", e);
            throw Throwables.rewrap(e);
        }

        if (pingedServiceUuid.equals(getUUID())) {
            log.error("Remote potential leader is claiming to be you!", e);
            throw Throwables.rewrap(e);
        }
    }

    @Override
    public String getUUID() {
        return proposer.getUUID();
    }

    @Override
    public boolean ping() {
        return isLastConfirmedLeader(knowledge.getGreatestLearnedValue());
    }

    private void proposeLeadership(LeadershipToken token) {
        lock.lock();
        try {
            PaxosValue value = knowledge.getGreatestLearnedValue();

            LeadershipToken expectedToken = genTokenFromValue(value);
            if (!expectedToken.sameAs(token)) {
                // This means that new data has come in so we shouldn't propose leadership.
                // We do this check in a lock to ensure concurrent callers to blockOnBecomingLeader behaves correctly.
                return;
            }

            long seq;
            if (value != null) {
                seq = value.getRound() + 1;
            } else {
                seq = Defaults.defaultValue(long.class);
            }

            proposer.propose(seq, null);
        } catch (PaxosRoundFailureException e) {
            // We have failed trying to become the leader.
            return;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public StillLeadingStatus isStillLeading(LeadershipToken token) {
        Preconditions.checkNotNull(token);

        final PaxosValue mostRecentValue = knowledge.getGreatestLearnedValue();
        final long seq = mostRecentValue.getRound();
        final LeadershipToken mostRecentToken = genTokenFromValue(mostRecentValue);

        // check if node thinks it is leader
        if (!isLastConfirmedLeader(mostRecentValue)) {
            return StillLeadingStatus.NOT_LEADING;
        }

        // check if token is invalidated
        if (!token.sameAs(mostRecentToken)) {
            return StillLeadingStatus.NOT_LEADING;
        }

        // check if node still has quorum
        List<PaxosResponse> responses = PaxosQuorumChecker.<PaxosAcceptor, PaxosResponse> collectQuorumResponses(
                acceptors,
                new Function<PaxosAcceptor, PaxosResponse>() {
                    @Override
                    @Nullable
                    public PaxosResponse apply(@Nullable PaxosAcceptor acceptor) {
                        return confirmLeader(acceptor, seq);
                    }
                },
                proposer.getQuorumSize(),
                executor,
                PaxosQuorumChecker.DEFAULT_REMOTE_REQUESTS_TIMEOUT_IN_SECONDS);
        if (PaxosQuorumChecker.hasQuorum(responses, proposer.getQuorumSize())) {
            // If we have a quorum we are good to go
            return StillLeadingStatus.LEADING;
        }

        for (PaxosResponse paxosResponse : responses) {
            if (paxosResponse != null && !paxosResponse.isSuccessful()) {
                // If we have a nack then someone has prepared or accepted a new seq.
                // In this case we are most likely not the leader
                return StillLeadingStatus.NOT_LEADING;
            }
        }
        return StillLeadingStatus.NO_QUORUM;
    }

    /**
     * Confirms if a given sequence is still the newest according to a given acceptor
     *
     * @param acceptor the acceptor to check against
     * @param seq the instance of paxos in question
     * @return a paxos response that either confirms the leader or nacks
     */
    private PaxosResponse confirmLeader(PaxosAcceptor acceptor, long seq) {
        return new BooleanPaxosResponse(seq >= acceptor.getLatestSequencePreparedOrAccepted());
    }

    public ImmutableList<PaxosAcceptor> getAcceptors() {
        return acceptors;
    }

    private boolean isLastConfirmedLeader(PaxosValue value) {
        return value != null ? value.getLeaderUUID().equals(proposer.getUUID()) : false;
    }

    /**
     * Queries all other learners for unknown learned values
     *
     * @param numPeersToQuery number of peer learners to query for updates
     * @returns true if new state was learned, otherwise false
     */
    public boolean updateLearnedStateFromPeers(PaxosValue greatestLearned) {
        final long nextToLearnSeq = greatestLearned != null ? greatestLearned.getRound() + 1 : Defaults.defaultValue(long.class);
        List<PaxosUpdate> updates = PaxosQuorumChecker.<PaxosLearner, PaxosUpdate> collectQuorumResponses(
                learners,
                new Function<PaxosLearner, PaxosUpdate>() {
                    @Override
                    @Nullable
                    public PaxosUpdate apply(@Nullable PaxosLearner learner) {
                        return new PaxosUpdate(
                                ImmutableList.copyOf(learner.getLearnedValuesSince(nextToLearnSeq)));
                    }
                },
                proposer.getQuorumSize(),
                executor,
                PaxosQuorumChecker.DEFAULT_REMOTE_REQUESTS_TIMEOUT_IN_SECONDS);

        // learn the state accumulated from peers
        boolean learned = false;
        for (PaxosUpdate update : updates) {
            ImmutableCollection<PaxosValue> values = update.getValues();
            for (PaxosValue value : values) {
                PaxosValue currentLearnedValue = knowledge.getLearnedValue(value.getRound());
                if (currentLearnedValue == null) {
                    knowledge.learn(value.getRound(), value);
                    learned = true;
                }
            }
        }

        return learned;
    }
}
