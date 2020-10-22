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
import com.palantir.lock.v2.LeaderTime;
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
    private final NamespacedConjureTimelockService delegate;
    private final LeaderElectionMetrics metrics;
    private final Clock clock;
    private volatile UUID leaderId = null;
    private Map<UUID, Instant> leadershipUpperBound = new ConcurrentHashMap<>();
    private Map<UUID, Instant> leadershipLowerBound = new ConcurrentHashMap<>();

    public LeaderElectionReportingTimelockService(
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
        return delegate.refreshLocks(request);
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
        return delegate.leaderTime();
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
     * Although from just one request, the lower bound is greater than the upper bound, after multiple responses that
     * is generally not going to be the case anymore.
     *
     * Ordering leaderships:
     * Let L_A and U_A be the lower and upper bound, respectively, of leader A, and let L_B and U_B be the respective
     * bounds of leader B. We distinguish 3 cases:
     *   1. (L_A < U_A && L_B < U_B): for both leaders there is an interval of guaranteed leadership, which by
     *   definitions cannot overlap. It is therefore trivial to order leadership of A and B.
     *   2. (w.l.o.g. L_A < U_A && L_B >= U_B):
     *      a) if U_B > U_A, A was the leader before B
     *      b) if L_B < L_A, B was the leader before A
     *      c) otherwise, we cannot determine the ordering
     *   3. (L_A >= U_A && L_B >= U_B):
     *      a) if U_B > L_A, A was the leader before B
     *      b) if U_A > L_B, B was the leader before A
     *      c) otherwise, we cannot determine the ordering
     *
     * Last leader election:
     * Given that the lower and upper bound for a leader are updated with each response from that leader, it is
     * unlikely the upper bound for a leader A will be lower than the lower bound except in the initial moments after
     * leadership has been acquired. We will therefore observe the last two leaderships where the older leader A has
     * had enough data points so that L_A < U_A. We will call these leaders long term leaders. We will therefore
     * consider either case 1) or case 2a) (when leadership was just gained) from above. Note that 2c) cannot happen
     * because we ignore such case, which is also both extremely unlikely and useless for this type of estimate.
     * There may be multiple leaders after A if only the last one was leader for long enough for its upper bound to
     * exceed the lower bound.
     *
     * Calculating the estimate:
     * Let A be the previous leader as described above and let L be the minimal lower bound of all considered leaders
     * that gained leadership after A. The estimated (over-approximated) duration of the leadership election is then
     * given by the duration between U_A and L, since L is the latest possible moment at which another leader was
     * elected while U_A is the earliest moment at which A could have lost leadership.
     */
    public Optional<Duration> calculateLastLeaderElectionDuration() {
        Map<UUID, Instant> lowerBoundSnapshot = ImmutableMap.copyOf(leadershipLowerBound.entrySet());
        Map<UUID, Instant> upperBoundSnapshot = ImmutableMap.copyOf(leadershipUpperBound.entrySet());

        Set<UUID> leaders = leadersWithBothBounds(lowerBoundSnapshot, upperBoundSnapshot);
        List<UUID> sortedLongTermLeaders = orderedLongTermLeaders(lowerBoundSnapshot, upperBoundSnapshot, leaders);

        if (sortedLongTermLeaders.isEmpty()) {
            return Optional.empty();
        }

        UUID lastLongTermLeader = sortedLongTermLeaders.get(sortedLongTermLeaders.size() - 1);

        // case 2a
        Optional<UUID> firstNextShortTermLeader = leaders.stream()
                .filter(id -> upperBoundSnapshot.get(id).isAfter(upperBoundSnapshot.get(lastLongTermLeader)))
                .min(Comparator.comparing(lowerBoundSnapshot::get));
        if (firstNextShortTermLeader.isPresent()) {
            clearOldLongTermLeadersExcept(sortedLongTermLeaders, 1);
            return Optional.of(estimateElectionDuration(
                    lowerBoundSnapshot, upperBoundSnapshot, lastLongTermLeader, firstNextShortTermLeader.get()));
        }

        if (sortedLongTermLeaders.size() == 1) {
            return Optional.empty();
        }

        // case 1
        clearOldLongTermLeadersExcept(sortedLongTermLeaders, 2);
        return Optional.of(estimateElectionDuration(
                lowerBoundSnapshot,
                upperBoundSnapshot,
                sortedLongTermLeaders.get(sortedLongTermLeaders.size() - 2),
                lastLongTermLeader));
    }

    private Set<UUID> leadersWithBothBounds(
            Map<UUID, Instant> lowerBoundSnapshot, Map<UUID, Instant> upperBoundSnapshot) {
        return upperBoundSnapshot.keySet().stream()
                .filter(lowerBoundSnapshot::containsKey)
                .collect(Collectors.toSet());
    }

    private List<UUID> orderedLongTermLeaders(
            Map<UUID, Instant> lowerBoundSnapshot,
            Map<UUID, Instant> upperBoundSnapshot,
            Set<UUID> leadersWithBothBounds) {
        return leadersWithBothBounds.stream()
                .filter(id -> upperBoundSnapshot.get(id).isAfter(lowerBoundSnapshot.get(id)))
                .sorted(Comparator.comparing(lowerBoundSnapshot::get))
                .collect(Collectors.toList());
    }

    private void clearOldLongTermLeadersExcept(List<UUID> sortedLongTermLeaders, int numberToRetain) {
        for (int i = 0; i < sortedLongTermLeaders.size() - numberToRetain; i++) {
            leadershipLowerBound.remove(sortedLongTermLeaders.get(i));
            leadershipUpperBound.remove(sortedLongTermLeaders.get(i));
        }
    }

    private Duration estimateElectionDuration(
            Map<UUID, Instant> lowerBoundSnapshot,
            Map<UUID, Instant> upperBoundSnapshot,
            UUID previousLeader,
            UUID nextLeader) {
        return Duration.between(upperBoundSnapshot.get(previousLeader), lowerBoundSnapshot.get(nextLeader));
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

    private void updateMetrics(Duration timeTaken) {
        metrics.observedDuration().update(timeTaken.toNanos(), TimeUnit.NANOSECONDS);
    }
}
