/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.paxos;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.RateLimiter;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.CheckedRejectedExecutionException;
import com.palantir.common.concurrent.CheckedRejectionExecutorService;
import com.palantir.common.concurrent.MultiplexingCompletionService;
import com.palantir.common.streams.KeyedStream;
import com.palantir.leader.PingResult;
import com.palantir.leader.PingableLeader;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.sls.versions.OrderableSlsVersion;
import com.palantir.sls.versions.VersionComparator;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import javax.sql.DataSource;

public final class SingleLeaderPinger implements LeaderPinger {
    private static final SafeLogger log = SafeLoggerFactory.get(SingleLeaderPinger.class);

    private final ConcurrentMap<UUID, LeaderPingerContext<PingableLeader>> uuidToServiceCache =
            new ConcurrentHashMap<>();
    private final Map<LeaderPingerContext<PingableLeader>, CheckedRejectionExecutorService> leaderPingExecutors;
    private final Duration leaderPingResponseWait;
    private final UUID localUuid;
    private final boolean cancelRemainingCalls;
    private final Optional<OrderableSlsVersion> timeLockVersion;
    private final RateLimiter pingV2RateLimiter = RateLimiter.create(1.0 / (5 * 60));
    private final GreenNodeLeadershipPrioritiser greenNodeLeadershipPrioritiser;

    private Map<LeaderPingerContext<PingableLeader>, Boolean> pingV2StatusOnRemotes = new HashMap<>();

    @VisibleForTesting
    SingleLeaderPinger(
            Map<LeaderPingerContext<PingableLeader>, CheckedRejectionExecutorService> otherPingableExecutors,
            Duration leaderPingResponseWait,
            UUID localUuid,
            boolean cancelRemainingCalls,
            Optional<OrderableSlsVersion> timeLockVersion,
            GreenNodeLeadershipPrioritiser greenNodeLeadershipPrioritiser) {
        this.leaderPingExecutors = otherPingableExecutors;
        this.leaderPingResponseWait = leaderPingResponseWait;
        this.localUuid = localUuid;
        this.cancelRemainingCalls = cancelRemainingCalls;
        this.timeLockVersion = timeLockVersion;
        this.greenNodeLeadershipPrioritiser = greenNodeLeadershipPrioritiser;
    }

    // VisibleForTesting
    public static SingleLeaderPinger createForTests(
            Map<LeaderPingerContext<PingableLeader>, CheckedRejectionExecutorService> otherPingableExecutors,
            Duration leaderPingResponseWait,
            UUID localUuid,
            boolean cancelRemainingCalls,
            Optional<OrderableSlsVersion> timeLockVersion) {
        GreenNodeLeadershipPrioritiser greenNodeLeadershipPrioritiser = new RateLimitedGreenNodeLeadershipPrioritiser();
        return new SingleLeaderPinger(
                otherPingableExecutors,
                leaderPingResponseWait,
                localUuid,
                cancelRemainingCalls,
                timeLockVersion,
                greenNodeLeadershipPrioritiser);
    }

    public static SingleLeaderPinger create(
            Map<LeaderPingerContext<PingableLeader>, CheckedRejectionExecutorService> otherPingableExecutors,
            DataSource sqliteDataSource,
            Duration leaderPingResponseWait,
            Supplier<Duration> greenNodeLeadershipBackoff,
            UUID localUuid,
            boolean cancelRemainingCalls,
            Optional<OrderableSlsVersion> timeLockVersion) {
        GreenNodeLeadershipPrioritiser greenNodeLeadershipPrioritiser =
                DbGreenNodeLeadershipPrioritiser.create(timeLockVersion, greenNodeLeadershipBackoff, sqliteDataSource);
        return new SingleLeaderPinger(
                otherPingableExecutors,
                leaderPingResponseWait,
                localUuid,
                cancelRemainingCalls,
                timeLockVersion,
                greenNodeLeadershipPrioritiser);
    }

    public static SingleLeaderPinger createLegacy(
            Map<LeaderPingerContext<PingableLeader>, ExecutorService> otherPingableExecutors,
            Duration leaderPingResponseWait,
            UUID localUuid,
            boolean cancelRemainingCalls) {
        GreenNodeLeadershipPrioritiser greenNodeLeadershipPrioritiser = new RateLimitedGreenNodeLeadershipPrioritiser();
        return new SingleLeaderPinger(
                KeyedStream.stream(otherPingableExecutors)
                        .map(CheckedRejectionExecutorService::new)
                        .collectToMap(),
                leaderPingResponseWait,
                localUuid,
                cancelRemainingCalls,
                Optional.empty(),
                greenNodeLeadershipPrioritiser);
    }

    @Override
    public LeaderPingResult pingLeaderWithUuid(UUID uuid) {
        Optional<LeaderPingerContext<PingableLeader>> suspectedLeader = getSuspectedLeader(uuid);
        if (!suspectedLeader.isPresent()) {
            return LeaderPingResults.pingReturnedFalse();
        }
        LeaderPingerContext<PingableLeader> leader = suspectedLeader.get();

        MultiplexingCompletionService<LeaderPingerContext<PingableLeader>, PingResult> multiplexingCompletionService =
                MultiplexingCompletionService.createFromCheckedExecutors(leaderPingExecutors);

        LeaderPingResult pingResult = null;

        if (shouldUsePingV2(leader)) {
            pingResult =
                    actuallyPingLeaderWithUuid(multiplexingCompletionService, uuid, leader, leader.pinger()::pingV2);
        }

        if (pingResult == null || pingResult.pingCallFailedDueToExecutionException()) {
            pingV2StatusOnRemotes.putIfAbsent(leader, false);
            pingResult = actuallyPingLeaderWithUuid(
                    multiplexingCompletionService, uuid, leader, () -> getPingResultFromLegacyEndpoint(leader));
        } else if (pingResult.pingCallWasSuccessfullyExecuted()) {
            pingV2StatusOnRemotes.put(leader, true);
        }

        return pingResult;
    }

    private boolean shouldUsePingV2(LeaderPingerContext<PingableLeader> leader) {
        return pingV2StatusOnRemotes.getOrDefault(leader, true) || pingV2RateLimiter.tryAcquire();
    }

    private PingResult getPingResultFromLegacyEndpoint(LeaderPingerContext<PingableLeader> leader) {
        return PingResult.builder().isLeader(leader.pinger().ping()).build();
    }

    private LeaderPingResult actuallyPingLeaderWithUuid(
            MultiplexingCompletionService<LeaderPingerContext<PingableLeader>, PingResult>
                    multiplexingCompletionService,
            UUID uuid,
            LeaderPingerContext<PingableLeader> leader,
            Callable<PingResult> pingEndpoint) {
        try {
            multiplexingCompletionService.submit(leader, pingEndpoint);
            Future<Map.Entry<LeaderPingerContext<PingableLeader>, PingResult>> pingFuture =
                    multiplexingCompletionService.poll(leaderPingResponseWait.toMillis(), TimeUnit.MILLISECONDS);
            return getLeaderPingResult(
                    uuid, pingFuture, timeLockVersion, greenNodeLeadershipPrioritiser::shouldGreeningNodeBecomeLeader);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return LeaderPingResults.pingCallFailure(e);
        } catch (CheckedRejectedExecutionException e) {
            log.warn("Could not ping the leader, because the executor used to talk to that node is overloaded", e);
            return LeaderPingResults.pingCallFailure(e);
        }
    }

    private static LeaderPingResult getLeaderPingResult(
            UUID uuid,
            @Nullable Future<Map.Entry<LeaderPingerContext<PingableLeader>, PingResult>> pingFuture,
            Optional<OrderableSlsVersion> timeLockVersion,
            BooleanSupplier shouldGreeningNodeBecomeLeader) {
        if (pingFuture == null) {
            return LeaderPingResults.pingTimedOut();
        }
        try {
            PingResult pingResult = Futures.getDone(pingFuture).getValue();
            if (!pingResult.isLeader()) {
                return LeaderPingResults.pingReturnedFalse();
            }
            return (!shouldGreeningNodeBecomeLeader.getAsBoolean() || isAtLeastOurVersion(pingResult, timeLockVersion))
                    ? LeaderPingResults.pingReturnedTrue(
                            uuid, Futures.getDone(pingFuture).getKey().hostAndPort())
                    : LeaderPingResults.pingReturnedTrueWithOlderVersion(
                            pingResult.timeLockVersion().get());
        } catch (ExecutionException e) {
            return LeaderPingResults.pingCallFailedWithExecutionException(e.getCause());
        }
    }

    private static boolean isAtLeastOurVersion(PingResult pingResult, Optional<OrderableSlsVersion> timeLockVersion) {
        return (pingResult.timeLockVersion().isPresent() && timeLockVersion.isPresent())
                ? VersionComparator.INSTANCE.compare(
                                pingResult.timeLockVersion().get(), timeLockVersion.get())
                        >= 0
                : true;
    }

    private Optional<LeaderPingerContext<PingableLeader>> getSuspectedLeader(UUID uuid) {
        if (uuidToServiceCache.containsKey(uuid)) {
            return Optional.of(uuidToServiceCache.get(uuid));
        }

        return getSuspectedLeaderOverNetwork(uuid);
    }

    private static class PaxosString implements PaxosResponse {

        private final String string;

        PaxosString(String string) {
            this.string = string;
        }

        @Override
        public boolean isSuccessful() {
            return true;
        }

        public String get() {
            return string;
        }
    }

    private Optional<LeaderPingerContext<PingableLeader>> getSuspectedLeaderOverNetwork(UUID uuid) {
        PaxosResponsesWithRemote<LeaderPingerContext<PingableLeader>, PaxosString> responses =
                PaxosQuorumChecker.collectUntil(
                        ImmutableList.copyOf(leaderPingExecutors.keySet()),
                        pingableLeader ->
                                new PaxosString(pingableLeader.pinger().getUUID()),
                        leaderPingExecutors,
                        leaderPingResponseWait,
                        state -> state.responses().values().stream()
                                .map(PaxosString::get)
                                .anyMatch(uuid.toString()::equals),
                        cancelRemainingCalls);

        for (Map.Entry<LeaderPingerContext<PingableLeader>, PaxosString> cacheEntry :
                responses.responses().entrySet()) {
            UUID uuidFromRequest = UUID.fromString(cacheEntry.getValue().get());
            LeaderPingerContext<PingableLeader> service =
                    uuidToServiceCache.putIfAbsent(uuidFromRequest, cacheEntry.getKey());
            throwIfInvalidSetup(service, cacheEntry.getKey(), uuidFromRequest);

            // return the leader if it matches
            if (uuid.equals(uuidFromRequest)) {
                return Optional.of(cacheEntry.getKey());
            }
        }

        return Optional.empty();
    }

    private void throwIfInvalidSetup(
            LeaderPingerContext<PingableLeader> cachedService,
            LeaderPingerContext<PingableLeader> pingedService,
            UUID pingedServiceUuid) {
        if (cachedService == null) {
            return;
        }

        IllegalStateException exception =
                new SafeIllegalStateException("There is a fatal problem with the leadership election configuration! "
                        + "This is probably caused by invalid pref files setting up the cluster "
                        + "(e.g. for lock server look at lock.prefs, leader.prefs, and lock_client.prefs)."
                        + "If the preferences are specified with a host port pair list and localhost index "
                        + "then make sure that the localhost index is correct (e.g. actually the localhost).");

        if (cachedService != pingedService) {
            log.error("Remote potential leaders are claiming to be each other!", exception);
            throw Throwables.rewrap(exception);
        }

        if (pingedServiceUuid.equals(localUuid)) {
            log.error("Remote potential leader is claiming to be you!", exception);
            throw Throwables.rewrap(exception);
        }
    }
}
