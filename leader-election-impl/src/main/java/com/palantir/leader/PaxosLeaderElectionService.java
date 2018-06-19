/*
 * Copyright 2015 Palantir Technologies
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
package com.palantir.leader;

import static com.google.common.collect.ImmutableList.copyOf;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Defaults;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.net.HostAndPort;
import com.palantir.common.base.Throwables;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosProposer;
import com.palantir.paxos.PaxosQuorumChecker;
import com.palantir.paxos.PaxosResponse;
import com.palantir.paxos.PaxosResponseImpl;
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
    private static final Logger leaderLog = LoggerFactory.getLogger("leadership");

    private final ReentrantLock lock;

    final PaxosProposer proposer;
    final PaxosLearner knowledge;

    final Map<PingableLeader, HostAndPort> potentialLeadersToHosts;
    final ImmutableList<PaxosAcceptor> acceptors;
    final ImmutableList<PaxosLearner> learners;

    final long updatePollingRateInMs;
    final long randomWaitBeforeProposingLeadership;
    final long leaderPingResponseWaitMs;

    final ExecutorService executor;

    final ConcurrentMap<String, PingableLeader> uuidToServiceCache = Maps.newConcurrentMap();

    @Deprecated // Use PaxosLeaderElectionServiceBuilder instead.
    public PaxosLeaderElectionService(PaxosProposer proposer,
                                      PaxosLearner knowledge,
                                      Map<PingableLeader, HostAndPort> potentialLeadersToHosts,
                                      List<PaxosAcceptor> acceptors,
                                      List<PaxosLearner> learners,
                                      ExecutorService executor,
                                      long updatePollingWaitInMs,
                                      long randomWaitBeforeProposingLeadership,
                                      long leaderPingResponseWaitMs) {
        this.proposer = proposer;
        this.knowledge = knowledge;
        // XXX This map uses something that may be proxied as a key! Be very careful if making a new map from this.
        this.potentialLeadersToHosts = Collections.unmodifiableMap(potentialLeadersToHosts);
        this.acceptors = copyOf(acceptors);
        this.learners = copyOf(learners);
        this.executor = executor;
        this.updatePollingRateInMs = updatePollingWaitInMs;
        this.randomWaitBeforeProposingLeadership = randomWaitBeforeProposingLeadership;
        this.leaderPingResponseWaitMs = leaderPingResponseWaitMs;
        lock = new ReentrantLock();
    }

    @Override
    public LeadershipToken blockOnBecomingLeader() throws InterruptedException {
        for (;;) {
            PaxosValue greatestLearned = knowledge.getGreatestLearnedValue();
            LeadershipToken token = genTokenFromValue(greatestLearned);

            if (isLastConfirmedLeader(greatestLearned)) {
                StillLeadingStatus leadingStatus = isStillLeading(token);
                if (leadingStatus == StillLeadingStatus.LEADING) {
                    log.info("Successfully became leader!");
                    return token;
                } else if (leadingStatus == StillLeadingStatus.NO_QUORUM) {
                    leaderLog.warn("The most recent known information says this server is the leader, but there is no quorum right now");
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
            log.debug("Waiting for [{}] ms before proposing leadership", backoffTime);
            Thread.sleep(backoffTime);

            proposeLeadership(token);
        }
    }

    private LeadershipToken genTokenFromValue(PaxosValue value) {
        return new PaxosLeadershipToken(value);
    }

    private boolean pingLeader() {
        Optional<PingableLeader> maybeLeader = getSuspectedLeader(true /* use network */);
        if (!maybeLeader.isPresent()) {
            return false;
        }
        final PingableLeader leader = maybeLeader.get();

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

    @Override
    public Optional<HostAndPort> getSuspectedLeaderInMemory() {
        Optional<PingableLeader> maybeLeader = getSuspectedLeader(false /* use network */);
        if (!maybeLeader.isPresent()) {
            return Optional.absent();
        }
        return Optional.of(potentialLeadersToHosts.get(maybeLeader.get()));
    }

    private Optional<PingableLeader> getSuspectedLeader(boolean useNetwork) {
        PaxosValue value = knowledge.getGreatestLearnedValue();
        if (value == null) {
            return Optional.absent();
        }

        // check leader cache
        String uuid = value.getLeaderUUID();
        if (uuidToServiceCache.containsKey(uuid)) {
            return Optional.of(uuidToServiceCache.get(uuid));
        }

        if (useNetwork) {
            return getSuspectedLeaderOverNetwork(uuid);
        } else {
            return Optional.absent();
        }
    }

    private Optional<PingableLeader> getSuspectedLeaderOverNetwork(String uuid) {
        CompletionService<Entry<String, PingableLeader>> pingService = new ExecutorCompletionService<Entry<String, PingableLeader>>(
                executor);

        // kick off requests to get leader uuids
        List<Future<Entry<String, PingableLeader>>> allFutures = Lists.newArrayList();
        for (final PingableLeader potentialLeader : potentialLeadersToHosts.keySet()) {
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

        return Optional.absent();
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

            log.debug("Proposing leadership with value [{}]", value);
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

            leaderLog.info("Proposing leadership with sequence number {}", seq);
            proposer.propose(seq, null);
        } catch (PaxosRoundFailureException e) {
            // We have failed trying to become the leader.
            leaderLog.warn("Leadership was not gained.\n"
                    + "We should recover automatically. If this recurs often, try to \n"
                    + "  (1) ensure that most other nodes are reachable over the network, and \n"
                    + "  (2) increase the randomWaitBeforeProposingLeadershipMs timeout in your configuration.\n"
                    + "See the debug-level log for more details.");
            leaderLog.debug("Specifically, leadership was not gained because of the following exception", e);
            return;
        } finally {
            lock.unlock();
        }
    }

    static class StillLeadingCall {
        private final AtomicInteger requestCount;
        private final CountDownLatch populationLatch;
        private volatile boolean failed;
        /* The status must only be written by the thread that owns and created this call. They must only be read by
         * other threads after awaiting the populationLatch.
         */
        private volatile StillLeadingStatus status;

        public StillLeadingCall() {
            this.requestCount = new AtomicInteger(1); // 1 because the current thread wants to make a request.
            // Counted down once by the thread that owns and created this call, after population or failure.
            this.populationLatch = new CountDownLatch(1);
            this.failed = false;
        }

        public void populate(StillLeadingStatus status) {
            this.status = status;
        }

        public int getRequestCountAndSetInvalid() {
            return requestCount.getAndSet(Integer.MIN_VALUE);
        }

        public void fail() {
            failed = true;
        }
        public void becomeReadable() {
            populationLatch.countDown();
        }
        // End creator-only threads.

        // This must only be called after awaitPopulation().
        public boolean isFailed() {
            return failed;
        }

        /**
         * @return true if we are included in the batch and false otherwise
         */
        public boolean joinBatch() {
            if (requestCount.get() < 0) {
                return false;
            }
            int val = requestCount.incrementAndGet();
            return isRequestCountValid(val);
        }

        public void awaitPopulation() {
            try {
                populationLatch.await();
            } catch (InterruptedException e) {
                log.error("Interrupted while waiting for a batch!", e);
                throw Throwables.throwUncheckedException(e);
            }
        }

        // This must only be called after awaitPopulation().
        public StillLeadingStatus getStatus() {
            return status;
        }

        public static boolean isRequestCountValid(int requestCount) {
            return requestCount > 0;
        }

    }

    /* The currently outstanding leadership check for a given token, if any. If it exists, it should be used, thus
     * batching remote calls. If there is none, one should be created and installed. The creator of a call must
     * populate it.
     */
    private final ConcurrentMap<LeadershipToken, StillLeadingCall> currentIsStillLeadingCall = Maps.newConcurrentMap();

    @Override
    public StillLeadingStatus isStillLeading(LeadershipToken token) {
        while (true) {
            StillLeadingCallBatch callBatch = getStillLeadingCallBatch(token);

            StillLeadingCall batch = callBatch.batch;
            if (callBatch.thisThreadOwnsBatch) {
                populateStillLeadingCall(batch, token);
            }

            batch.awaitPopulation();

            // Now the batch is ready to be read.
            if (!batch.isFailed()) {
                return batch.getStatus();
            }
        }
    }

    private static class StillLeadingCallBatch {
        public final boolean thisThreadOwnsBatch;
        public final StillLeadingCall batch;
        public StillLeadingCallBatch(boolean thisThreadOwnsBatch,
                                     StillLeadingCall batch) {
            this.thisThreadOwnsBatch = thisThreadOwnsBatch;
            this.batch = batch;
        }
    }
    private final static int MAX_INSTALL_BATCH_ATTEMPTS = 5;
    private StillLeadingCallBatch getStillLeadingCallBatch(LeadershipToken token) {
        boolean installedNewBatch = false;
        boolean joinedBatch = false;
        @Nullable StillLeadingCall batch = null;
        for (int installBatchAttempts = 0; !(installedNewBatch || joinedBatch); installBatchAttempts++) {
            batch = currentIsStillLeadingCall.get(token);
            if (batch != null) {
                joinedBatch = batch.joinBatch();
            }
            if (!joinedBatch) {
                StillLeadingCall newBatch = new StillLeadingCall();
                if (installBatchAttempts <= MAX_INSTALL_BATCH_ATTEMPTS) {
                    if (batch == null) {
                        installedNewBatch = currentIsStillLeadingCall.putIfAbsent(token, newBatch) == null;
                    } else {
                        installedNewBatch = currentIsStillLeadingCall.replace(token, batch, newBatch);
                    }
                } else {
                    // Barge ahead in this case to prevent very unfortunate scheduling from preventing progress.
                    log.warn("Failed to install a leadership check batch {} times; blindly installing a batch. " +
                            "This should be rare!", MAX_INSTALL_BATCH_ATTEMPTS);
                    currentIsStillLeadingCall.put(token, newBatch);
                    installedNewBatch = true;
                }
                if (installedNewBatch) {
                    // wait for in-flight batch to finish before continuing (simple batching)
                    if (batch != null) {
                        batch.awaitPopulation();
                    }
                    batch = newBatch;
                }
            }
        }
        Preconditions.checkState(batch != null);
        return new StillLeadingCallBatch(installedNewBatch, batch);
    }

    private void populateStillLeadingCall(StillLeadingCall batch, LeadershipToken token) {
        try {
            batch.getRequestCountAndSetInvalid();
            StillLeadingStatus status = isStillLeadingInternal(token);
            batch.populate(status);
        } catch (Throwable t) {
            log.error("Something went wrong while checking leadership", t);
            batch.fail();
        } finally {
            batch.becomeReadable();
            /* Close only the current batch. If someone else has replaced this batch
             * already, don't close them prematurely; let them close themselves.
             */
            currentIsStillLeadingCall.remove(token, batch);
        }
    }


    private StillLeadingStatus isStillLeadingInternal(LeadershipToken token) {
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
                PaxosQuorumChecker.DEFAULT_REMOTE_REQUESTS_TIMEOUT_IN_SECONDS,
                true);
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
        return new PaxosResponseImpl(seq >= acceptor.getLatestSequencePreparedOrAccepted());
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
                                copyOf(learner.getLearnedValuesSince(nextToLearnSeq)));
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
