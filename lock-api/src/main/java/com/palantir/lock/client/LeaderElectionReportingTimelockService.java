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

package com.palantir.lock.client;

import com.codahale.metrics.Snapshot;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsRequest;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsResponse;
import com.palantir.atlasdb.timelock.api.ConjureLockRequest;
import com.palantir.atlasdb.timelock.api.ConjureLockResponse;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksRequest;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksResponse;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsRequest;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.atlasdb.timelock.api.ConjureTimelockService;
import com.palantir.atlasdb.timelock.api.ConjureUnlockRequest;
import com.palantir.atlasdb.timelock.api.ConjureUnlockResponse;
import com.palantir.atlasdb.timelock.api.ConjureWaitForLocksResponse;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsRequest;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsResponse;
import com.palantir.common.time.Clock;
import com.palantir.conjure.java.lib.SafeLong;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.timelock.feedback.LeaderElectionDuration;
import com.palantir.timelock.feedback.LeaderElectionStatistics;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class LeaderElectionReportingTimelockService implements NamespacedConjureTimelockService {
    private static final SafeLogger log = SafeLoggerFactory.get(LeaderElectionReportingTimelockService.class);

    private final NamespacedConjureTimelockService delegate;
    private volatile LeaderElectionMetrics metrics;
    private final Clock clock;
    private volatile UUID leaderId = null;
    private volatile LeaderElectionDuration lastComputedDuration = null;
    private Map<UUID, Instant> leadershipUpperBound = new ConcurrentHashMap<>();
    private Map<UUID, Instant> leadershipLowerBound = new ConcurrentHashMap<>();

    @VisibleForTesting
    LeaderElectionReportingTimelockService(
            NamespacedConjureTimelockService delegate, TaggedMetricRegistry taggedMetricRegistry, Clock clock) {
        this.delegate = delegate;
        this.metrics = LeaderElectionMetrics.of(taggedMetricRegistry);
        this.clock = clock;
    }

    public static LeaderElectionReportingTimelockService create(
            ConjureTimelockService conjureTimelockService, String namespace) {
        return new LeaderElectionReportingTimelockService(
                new NamespacedConjureTimelockServiceImpl(conjureTimelockService, namespace),
                new DefaultTaggedMetricRegistry(),
                System::currentTimeMillis);
    }

    @Override
    public ConjureUnlockResponse unlock(ConjureUnlockRequest request) {
        return delegate.unlock(request);
    }

    @Override
    public ConjureRefreshLocksResponse refreshLocks(ConjureRefreshLocksRequest request) {
        return runTimed(
                () -> delegate.refreshLocks(request),
                response -> response.getLease().leaderTime().id().id());
    }

    @Override
    public ConjureWaitForLocksResponse waitForLocks(ConjureLockRequest request) {
        return delegate.waitForLocks(request);
    }

    @Override
    public ConjureLockResponse lock(ConjureLockRequest request) {
        return delegate.lock(request);
    }

    @Override
    public LeaderTime leaderTime() {
        return runTimed(delegate::leaderTime, response -> response.id().id());
    }

    @Override
    public GetCommitTimestampsResponse getCommitTimestamps(GetCommitTimestampsRequest request) {
        return runTimed(() -> delegate.getCommitTimestamps(request), response -> response.getLockWatchUpdate()
                .logId());
    }

    @Override
    public ConjureGetFreshTimestampsResponse getFreshTimestamps(ConjureGetFreshTimestampsRequest request) {
        return delegate.getFreshTimestamps(request);
    }

    @Override
    public ConjureStartTransactionsResponse startTransactions(ConjureStartTransactionsRequest request) {
        return runTimed(() -> delegate.startTransactions(request), response -> response.getLockWatchUpdate()
                .logId());
    }

    public LeaderElectionStatistics getStatistics() {
        return getStatisticsAndSetRegistryTo(new DefaultTaggedMetricRegistry());
    }

    @VisibleForTesting
    LeaderElectionStatistics getStatisticsAndSetRegistryTo(TaggedMetricRegistry metricRegistry) {
        Snapshot metricsSnapshot = metrics.observedDuration().getSnapshot();
        LeaderElectionStatistics electionStatistics = LeaderElectionStatistics.builder()
                .p99(metricsSnapshot.get99thPercentile())
                .p95(metricsSnapshot.get95thPercentile())
                .mean(metricsSnapshot.getMean())
                .count(SafeLong.of(metricsSnapshot.size()))
                .durationEstimate(calculateLastLeaderElectionDuration())
                .build();
        metrics = LeaderElectionMetrics.of(metricRegistry);
        return electionStatistics;
    }

    private <T> T runTimed(Supplier<T> method, Function<T, UUID> leaderExtractor) {
        UUID currentLeader = leaderId;
        Instant startTime = clock.instant();
        T response = method.get();
        Instant responseTime = clock.instant();
        if (updateAndChangeLeaderIfNew(leaderExtractor, currentLeader, response, startTime, responseTime)) {
            updateMetrics(Duration.between(startTime, responseTime));
        }
        return response;
    }

    private <T> boolean updateAndChangeLeaderIfNew(
            Function<T, UUID> leaderExtractor,
            UUID currentLeader,
            T response,
            Instant startTime,
            Instant responseTime) {
        UUID newLeader = leaderExtractor.apply(response);
        leadershipUpperBound.compute(newLeader, (ignore, oldTime) -> max(oldTime, startTime));
        leadershipLowerBound.compute(newLeader, (ignore, oldTime) -> min(oldTime, responseTime));
        boolean election = !newLeader.equals(currentLeader);

        if (election) {
            if (!newLeader.equals(leaderId)) {
                log.info(
                        "Apparent leader change from {} to {}.",
                        SafeArg.of("old leader", leaderId),
                        SafeArg.of("new leader", newLeader));
            }
            leaderId = newLeader;
        }

        return election && (currentLeader != null);
    }

    /**
     * Estimating the duration of leader election:
     *
     * If a request is sent out at time T_1, and a response arrives at time T_2 with the id of leader A, we can
     * deduce three things:
     *   1. A could not have lost leadership before T_1 if it was the leader before it (upper bound)
     *   2. A must have gained leadership before T_2 (lower bound)
     *   3. A was the leader at least in some point in the interval [T_1, T_2]
     * T_1 is the upper bound in the sense that if we know that A was the leader before T_1, we know for certain it was
     * still the leader until T_1. Similarly, T_2 is the lower bound in the sense that if A was not the leader at any
     * point after T_2, it was certainly the leader at T_2. This is slightly counter-intuitive with just a single
     * request, since T_1 < T_2, but as soon as another response is received such that T_1' > T_2, we can determine an
     * interval [T_2, T_1'] when A was definitely the leader. Let us denote this interval by [L_A, U_A] and call A a
     * long term leader.
     *
     * Last leader election:
     * Given that the lower and upper bound for a leader are updated with each response from that leader, it is
     * unlikely that any leader is not going to be a long term leader except in the initial moments after leadership
     * has been acquired. We will therefore observe the last two leaderships where the older leader A has had enough
     * data points so that L_A < U_A, while the newer leader B is allowed to not have become a long term leader yet.
     *
     * Ordering leaderships:
     * Let A be a long term leader and let B another leader. We distinguish 2 cases:
     *   1. (B is a long term leader): for both leaders there is an interval of guaranteed leadership, which by
     *   definition cannot overlap. It is therefore trivial to order leadership of A and B.
     *   2. (B is not a long term leader yet, i.e., L_B >= U_B):
     *      a) if U_B > U_A, A was the leader before B
     *      b) if L_B < L_A, B was the leader before A
     *      c) otherwise, we cannot determine the ordering
     *
     * Calculating the estimate:
     * Let A be a long term leader and let L be the minimal lower bound of all leaders that became leaders after A as
     * described in 1) and 2b). The estimated (over-approximated) duration of the leadership election is then
     * given by the duration between U_A and L, since L is the latest possible moment at which another leader was
     * elected while U_A is the earliest moment at which A could have lost leadership. This method will always return
     * the duration of the most recent such interval.
     */
    @VisibleForTesting
    Optional<LeaderElectionDuration> calculateLastLeaderElectionDuration() {
        Map<UUID, Instant> lowerBounds = ImmutableMap.copyOf(leadershipLowerBound.entrySet());
        Map<UUID, Instant> upperBounds = ImmutableMap.copyOf(leadershipUpperBound.entrySet());

        Set<UUID> leaders = leadersWithBothBounds(lowerBounds, upperBounds);
        List<UUID> sortedLongTermLeaders = orderedLongTermLeaders(lowerBounds, upperBounds, leaders);
        clearOldLongTermLeaders(sortedLongTermLeaders);

        if (sortedLongTermLeaders.isEmpty()) {
            return Optional.empty();
        }

        UUID lastLongTermLeader = sortedLongTermLeaders.get(sortedLongTermLeaders.size() - 1);

        Optional<LeaderElectionDuration> result =
                durationToNextLeader(lowerBounds, upperBounds, leaders, lastLongTermLeader);
        if (result.isPresent() || sortedLongTermLeaders.size() == 1) {
            return result;
        }

        UUID secondToLastLongTermLeader = sortedLongTermLeaders.get(sortedLongTermLeaders.size() - 2);
        Optional<LeaderElectionDuration> leaderElectionDuration =
                durationToNextLeader(lowerBounds, upperBounds, leaders, secondToLastLongTermLeader);
        if (leaderElectionDuration.isPresent() && !leaderElectionDuration.get().equals(lastComputedDuration)) {
            lastComputedDuration = leaderElectionDuration.get();
            log.info(
                    "Computed new leader election duration estimate {}.",
                    SafeArg.of("leader election duration", leaderElectionDuration.get()));
        }
        return leaderElectionDuration;
    }

    private void clearOldLongTermLeaders(List<UUID> sortedLongTermLeaders) {
        for (int i = 0; i < sortedLongTermLeaders.size() - 2; i++) {
            leadershipLowerBound.remove(sortedLongTermLeaders.get(i));
            leadershipUpperBound.remove(sortedLongTermLeaders.get(i));
            log.info("Cleared old long term leader {}.", SafeArg.of("id", sortedLongTermLeaders.get(i)));
        }
    }

    private void updateMetrics(Duration timeTaken) {
        metrics.observedDuration().update(timeTaken.toNanos(), TimeUnit.NANOSECONDS);
    }

    private static Set<UUID> leadersWithBothBounds(Map<UUID, Instant> lowerBounds, Map<UUID, Instant> upperBounds) {
        return upperBounds.keySet().stream().filter(lowerBounds::containsKey).collect(Collectors.toSet());
    }

    private static List<UUID> orderedLongTermLeaders(
            Map<UUID, Instant> lowerBounds, Map<UUID, Instant> upperBounds, Set<UUID> leadersWithBothBounds) {
        return leadersWithBothBounds.stream()
                .filter(id -> upperBounds.get(id).isAfter(lowerBounds.get(id)))
                .sorted(Comparator.comparing(lowerBounds::get))
                .collect(Collectors.toList());
    }

    private static Optional<LeaderElectionDuration> durationToNextLeader(
            Map<UUID, Instant> lowerBounds,
            Map<UUID, Instant> upperBounds,
            Set<UUID> leaders,
            UUID lastLongTermLeader) {
        Optional<UUID> firstNextShortTermLeader = leaders.stream()
                .filter(id -> upperBounds.get(id).isAfter(upperBounds.get(lastLongTermLeader)))
                .min(Comparator.comparing(lowerBounds::get));
        return firstNextShortTermLeader.map(newLeader -> LeaderElectionDuration.builder()
                .oldLeader(lastLongTermLeader)
                .newLeader(newLeader)
                .duration(estimateElectionDuration(lowerBounds, upperBounds, lastLongTermLeader, newLeader))
                .build());
    }

    private static Duration estimateElectionDuration(
            Map<UUID, Instant> lowerBounds, Map<UUID, Instant> upperBounds, UUID previousLeader, UUID nextLeader) {
        return Duration.between(upperBounds.get(previousLeader), lowerBounds.get(nextLeader));
    }

    private static Instant max(Instant first, @Nonnull Instant second) {
        if (first == null) {
            return second;
        }
        return first.isAfter(second) ? first : second;
    }

    private static Instant min(Instant first, @Nonnull Instant second) {
        if (first == null) {
            return second;
        }
        return first.isBefore(second) ? first : second;
    }
}
