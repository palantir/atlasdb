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

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Histogram;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.FluentFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.paxos.LeaderPingResult;
import com.palantir.paxos.LeaderPingResults;
import com.palantir.paxos.LeaderPingerContext;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.SharedTaggedMetricRegistries;

final class ManualBatchingPingableLeader extends AbstractScheduledService implements ClientAwarePingableLeader {

    private static final Logger log = LoggerFactory.getLogger(ManualBatchingPingableLeader.class);

    private final LeaderPingerContext<BatchPingableLeader> remoteClient;
    private final Duration leaderPingRate;
    private final Duration leaderPingResponseWait;
    private final UUID nodeUuid;

    private final Map<Client, SettableFuture<Void>> hasProcessedFirstRequest = Maps.newConcurrentMap();
    private final AtomicReference<LastResult> lastResult = new AtomicReference<>();
    private final Histogram histogram;

    ManualBatchingPingableLeader(
            LeaderPingerContext<BatchPingableLeader> remoteClient,
            Duration leaderPingRate,
            Duration leaderPingResponseWait,
            UUID nodeUuid) {
        this.remoteClient = remoteClient;
        this.leaderPingRate = leaderPingRate;
        this.leaderPingResponseWait = leaderPingResponseWait;
        this.nodeUuid = nodeUuid;

        // TODO(fdesouza): for comparison whilst testing
        MetricName metricName = MetricName.builder()
                .safeName("atlasdb.autobatcherMeter")
                .putSafeTags("identifier", "batch-pingable-leader.ping")
                .putSafeTags("uuid", nodeUuid.toString())
                .putSafeTags("remoteHostAndPort", remoteClient.hostAndPort().toString())
                .build();
        this.histogram = SharedTaggedMetricRegistries.getSingleton().histogram(metricName);
    }

    @Override
    public Future<LeaderPingResult> ping(UUID requestedUuid, Client client) {
        // register client with the batcher to check whether or not the remote client is the leader or not
        // since there is a case where we'll miss the window, we must wait until the client has been registered and
        // been included in an existing batch
        // wait up to leaderPingResponseRate each time until it has been included and requested in a batch
        Instant requestTime = Instant.now();

        return FluentFuture.from(hasProcessedFirstRequest.computeIfAbsent(client, $ -> SettableFuture.create()))
                .transform(
                        // this will never be null, since we waited for our request to be processed at least once
                        // semantic difference here is that values are effectively cached for leaderPingResponseWait
                        // in the event that things are not good, but in the bigger picture these *should* be
                        // equivalent.
                        $ -> {
                            // if for any reason we've stopped, bubble up the exception
                            checkNotShutdown();
                            return lastResult.get().result(
                                    client,
                                    requestTime.minus(leaderPingResponseWait),
                                    remoteClient.hostAndPort(),
                                    requestedUuid);
                        },
                        MoreExecutors.directExecutor());
    }

    private void checkNotShutdown() {
        State state = state();
        Preconditions.checkState(state != State.STOPPING && state != State.TERMINATED,
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
                log.info("Ping took more than ping response wait, any waiters will report that ping timing out {}",
                        SafeArg.of("pingDuration", pingDuration),
                        SafeArg.of("leaderPingResponseWait", leaderPingResponseWait));
            }
            LastResult newResult = ImmutableLastResult.of(after, clientsThisNodeIsTheLeaderFor);
            this.lastResult.set(newResult);

            histogram.update(clientsToCheck.size());
            clientsToCheck.forEach(clientJustProcessed -> hasProcessedFirstRequest.get(clientJustProcessed).set(null));
        } catch (Exception e) {
            // TODO(fdesouza): should this also set the result to include any exceptions??
            //  Means no caching of last good result in the presence of errors
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
    interface LastResult {
        @Value.Parameter
        Instant timestamp();
        @Value.Parameter
        Set<Client> clientsNodeIsTheLeaderFor();

        default LeaderPingResult result(
                Client client,
                Instant earliestCompletedByDeadline,
                HostAndPort hostAndPort,
                UUID requestedUuid) {
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
