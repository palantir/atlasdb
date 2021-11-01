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

package com.palantir.atlasdb.timelock.paxos;

import com.codahale.metrics.Histogram;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.FluentFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.paxos.Client;
import com.palantir.paxos.LeaderPingResult;
import com.palantir.paxos.LeaderPingResults;
import com.palantir.paxos.LeaderPingerContext;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.SharedTaggedMetricRegistries;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.immutables.value.Value;

/**
 * Clients register their intent to check whether the given remote is their leader. Periodically, the total set of
 * clients that have been registered during the lifetime of this service will be checked. Successful results are cached
 * up to {@code leaderPingResponseWait} after which a time out will be issued if no further pings take place.
 */
final class CumulativeLeaderPinger extends AbstractScheduledService implements ClientAwareLeaderPinger {

    private static final SafeLogger log = SafeLoggerFactory.get(CumulativeLeaderPinger.class);

    private final LeaderPingerContext<BatchPingableLeader> remoteClient;
    private final Duration leaderPingRate;
    private final Duration leaderPingResponseWait;
    private final UUID nodeUuid;

    private final Map<Client, SettableFuture<Void>> hasProcessedFirstRequest = new ConcurrentHashMap<>();
    private final AtomicReference<LastSuccessfulResult> lastSuccessfulResult = new AtomicReference<>();
    private final Histogram histogram;

    CumulativeLeaderPinger(
            LeaderPingerContext<BatchPingableLeader> remoteClient,
            Duration leaderPingRate,
            Duration leaderPingResponseWait,
            UUID nodeUuid) {
        this.remoteClient = remoteClient;
        this.leaderPingRate = leaderPingRate;
        this.leaderPingResponseWait = leaderPingResponseWait;
        this.nodeUuid = nodeUuid;

        // for comparison to the previous autobatching version, can easily remove later
        MetricName metricName = MetricName.builder()
                .safeName("atlasdb.autobatcherMeter")
                .putSafeTags("identifier", "batch-pingable-leader.ping")
                .putSafeTags("remoteHostAndPort", remoteClient.hostAndPort().toString())
                .build();
        this.histogram = SharedTaggedMetricRegistries.getSingleton().histogram(metricName);
    }

    @Override
    public Future<LeaderPingResult> registerAndPing(UUID requestedUuid, Client client) {
        // Register the client to be checked eventually. In the event that the client has not been in a ping request,
        // we return a future that will succeed when the *next* ping succeeds. In the steady state, this reduces to an
        // in memory read of lastSuccessfulResult.

        Instant requestTime = Instant.now();

        return FluentFuture.from(hasProcessedFirstRequest.computeIfAbsent(client, _$ -> SettableFuture.create()))
                .transform(
                        // lastSuccessfulResult will never be null, since the SettableFuture in the above will have been
                        // set after a ping containing the client is made.
                        _$ -> {
                            checkNotShutdown();
                            return lastSuccessfulResult
                                    .get()
                                    .result(
                                            client,
                                            requestTime.minus(leaderPingResponseWait),
                                            remoteClient.hostAndPort(),
                                            requestedUuid);
                        },
                        MoreExecutors.directExecutor());
    }

    private void checkNotShutdown() {
        State state = state();
        Preconditions.checkState(
                state != State.STOPPING && state != State.TERMINATED,
                "pinger is either shutdown or in the process of shutting down");
    }

    @Override
    protected String serviceName() {
        return String.format("ManualBatchingPingableLeader %s -> %s", nodeUuid.toString(), remoteClient.hostAndPort());
    }

    @Override
    public LeaderPingerContext<BatchPingableLeader> underlyingRpcClient() {
        return remoteClient;
    }

    @Override
    protected void runOneIteration() {
        try {
            Set<Client> clientsToCheck = ImmutableSet.copyOf(hasProcessedFirstRequest.keySet());
            Instant before = Instant.now();
            Set<Client> clientsThisNodeIsTheLeaderFor = remoteClient.pinger().ping(clientsToCheck);
            Instant after = Instant.now();

            Duration pingDuration = Duration.between(before, after);
            if (pingDuration.compareTo(leaderPingResponseWait) >= 0) {
                log.info(
                        "Ping took more than ping response wait, any waiters will report that ping timed out",
                        SafeArg.of("pingDuration", pingDuration),
                        SafeArg.of("leaderPingResponseWait", leaderPingResponseWait));
            }
            LastSuccessfulResult newResult = ImmutableLastSuccessfulResult.of(after, clientsThisNodeIsTheLeaderFor);
            this.lastSuccessfulResult.set(newResult);

            histogram.update(clientsToCheck.size());
            clientsToCheck.forEach(clientJustProcessed ->
                    hasProcessedFirstRequest.get(clientJustProcessed).set(null));
        } catch (Exception e) {
            log.warn("Failed to ping node, trying again in the next round", e);
        }
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedDelaySchedule(0, leaderPingRate.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        stopAsync();
    }

    @Value.Immutable
    interface LastSuccessfulResult {
        @Value.Parameter
        Instant timestamp();

        @Value.Parameter
        Set<Client> clientsNodeIsTheLeaderFor();

        default LeaderPingResult result(
                Client client, Instant earliestCompletedByDeadline, HostAndPort hostAndPort, UUID requestedUuid) {
            if (timestamp().isBefore(earliestCompletedByDeadline)) {
                return LeaderPingResults.pingTimedOut();
            }

            if (clientsNodeIsTheLeaderFor().contains(client)) {
                return LeaderPingResults.pingReturnedTrue(requestedUuid, hostAndPort);
            } else {
                return LeaderPingResults.pingReturnedFalse();
            }
        }
    }
}
