/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.cassandra.pool.CassandraServer;
import com.palantir.conjure.java.api.config.service.HumanReadableDuration;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import one.util.streamex.EntryStream;
import org.apache.thrift.transport.TTransportException;
import org.immutables.value.Value;

public final class CassandraTopologyValidator {
    private static final SafeLogger log = SafeLoggerFactory.get(CassandraTopologyValidator.class);

    private final CassandraTopologyValidationMetrics metrics;

    public CassandraTopologyValidator(CassandraTopologyValidationMetrics metrics) {
        this.metrics = metrics;
    }

    public Set<CassandraServer> getNewHostsWithInconsistentTopologiesAndRetry(
            Set<CassandraServer> newlyAddedHosts,
            Map<CassandraServer, CassandraClientPoolingContainer> allHosts,
            HumanReadableDuration waitTimeBetweenCalls,
            HumanReadableDuration maxWaitTime) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        Retryer<Set<CassandraServer>> retryer = RetryerBuilder.<Set<CassandraServer>>newBuilder()
                .retryIfResult(servers -> servers.size() == allHosts.size())
                .retryIfException()
                .withWaitStrategy(
                        WaitStrategies.fixedWait(waitTimeBetweenCalls.toMilliseconds(), TimeUnit.MILLISECONDS))
                .withStopStrategy(StopStrategies.stopAfterDelay(maxWaitTime.toMilliseconds(), TimeUnit.MILLISECONDS))
                .build();
        try {
            return retryer.call(() -> getNewHostsWithInconsistentTopologies(newlyAddedHosts, allHosts));
        } catch (ExecutionException | RetryException e) {
            metrics.markTopologyValidationFailure();
            log.error(
                    "Failed to obtain consistent view of hosts from cluster.",
                    SafeArg.of("newlyAddedCassandraHosts", newlyAddedHosts),
                    SafeArg.of("allCassandraHosts", allHosts.keySet()),
                    e);
            throw new SafeRuntimeException("Failed to obtain consistent view of hosts from cluster.", e);
        } finally {
            metrics.recordTopologyValidationLatency(Duration.ofMillis(stopwatch.elapsed(TimeUnit.MILLISECONDS)));
        }
    }

    public Set<CassandraServer> getNewHostsWithInconsistentTopologies(
            Set<CassandraServer> newlyAddedHosts, Map<CassandraServer, CassandraClientPoolingContainer> allHosts) {

        if (newlyAddedHosts.isEmpty()) {
            return newlyAddedHosts;
        }

        Preconditions.checkArgument(
                Sets.difference(newlyAddedHosts, allHosts.keySet()).isEmpty(),
                "Newly added hosts must be a subset of all hosts.",
                SafeArg.of("newlyAddedHosts", newlyAddedHosts),
                SafeArg.of("allHosts", allHosts));

        Map<CassandraServer, CassandraClientPoolingContainer> currentServers =
                EntryStream.of(allHosts).removeKeys(newlyAddedHosts::contains).toMap();
        Map<CassandraServer, CassandraClientPoolingContainer> newServers =
                EntryStream.of(allHosts).filterKeys(newlyAddedHosts::contains).toMap();

        ClusterTopologyResult currentClusterTopology = getClusterTopology(currentServers);

        // Do not add any new hosts as we are not in consensus currently
        if (currentClusterTopology.consensus().equals(ClusterTopologyResult.Consensus.NO_CONSENSUS)) {
            return newlyAddedHosts;
        }

        if (currentClusterTopology.hostIds().isEmpty()) {
            ClusterTopologyResult newServersClusterTopology = getClusterTopology(newServers);

            if (newServersClusterTopology.consensus().equals(ClusterTopologyResult.Consensus.NO_CONSENSUS)) {
                return newlyAddedHosts;
            }

            // Remove only the servers which were not in consensus
            return Sets.difference(newlyAddedHosts, newServersClusterTopology.servers());
        }

        return EntryStream.of(fetchHostIdsForServers(newServers))
                .removeValues(hostIds -> hostIds.equals(currentClusterTopology.hostIds()))
                .keys()
                .toSet();
    }

    private ClusterTopologyResult getClusterTopology(Map<CassandraServer, CassandraClientPoolingContainer> hosts) {
        Map<CassandraServer, Set<String>> hostIdsByServer = fetchHostIdsForServers(hosts);
        if (hostIdsByServer.isEmpty()) {
            return ClusterTopologyResult.of(ClusterTopologyResult.Consensus.HAS_CONSENSUS, Set.of(), hosts.keySet());
        }

        Map<CassandraServer, Set<String>> hostIdsWithValues =
                EntryStream.of(hostIdsByServer).removeValues(Set::isEmpty).toMap();

        int quorum = (hostIdsByServer.size() + 1) / 2;

        if (hostIdsWithValues.size() < quorum) {
            return ClusterTopologyResult.of(ClusterTopologyResult.Consensus.NO_CONSENSUS, Set.of(), Set.of());
        }

        Set<Set<String>> uniqueSetsOfHostIds =
                EntryStream.of(hostIdsWithValues).values().toImmutableSet();

        if (uniqueSetsOfHostIds.size() == 1) {
            Set<String> uniqueHostIds = Iterables.getOnlyElement(uniqueSetsOfHostIds);
            Set<CassandraServer> cassandraServersWithMatchingHostIds = EntryStream.of(hostIdsByServer)
                    .filterValues(hostIds -> hostIds.equals(uniqueHostIds))
                    .keys()
                    .toSet();
            Set<CassandraServer> cassandraServerWithoutHostIdEndpoint =
                    Sets.difference(hosts.keySet(), hostIdsByServer.keySet());
            return ClusterTopologyResult.of(
                    ClusterTopologyResult.Consensus.HAS_CONSENSUS,
                    uniqueHostIds,
                    Sets.union(cassandraServersWithMatchingHostIds, cassandraServerWithoutHostIdEndpoint));
        }

        return ClusterTopologyResult.of(ClusterTopologyResult.Consensus.NO_CONSENSUS, Set.of(), Set.of());
    }

    @VisibleForTesting
    Optional<Set<String>> fetchHostIds(CassandraClientPoolingContainer container) {
        try {
            return container.<Optional<Set<String>>, Exception>runWithPooledResource(
                    client -> Optional.of(ImmutableSet.copyOf(client.get_host_ids())));
        } catch (TTransportException e) {
            log.warn(
                    "Failed to get host ids from host due to transport exception",
                    SafeArg.of("host", container.getCassandraServer().cassandraHostName()),
                    SafeArg.of(
                            "proxy",
                            CassandraLogHelper.host(
                                    container.getCassandraServer().proxy())),
                    e);
            return Optional.of(Set.of());
        } catch (Exception e) {
            log.warn(
                    "Failed to get host ids from host",
                    SafeArg.of("host", container.getCassandraServer().cassandraHostName()),
                    SafeArg.of(
                            "proxy",
                            CassandraLogHelper.host(
                                    container.getCassandraServer().proxy())),
                    e);
            return Optional.empty();
        }
    }

    private Map<CassandraServer, Set<String>> fetchHostIdsForServers(
            Map<CassandraServer, CassandraClientPoolingContainer> servers) {
        return EntryStream.of(servers)
                .mapValues(this::fetchHostIds)
                .removeValues(Optional::isEmpty)
                .mapValues(Optional::get)
                .toMap();
    }

    @Value.Immutable
    public interface ClusterTopologyResult {
        enum Consensus {
            HAS_CONSENSUS,
            NO_CONSENSUS
        }

        Consensus consensus();

        Set<String> hostIds();

        Set<CassandraServer> servers();

        static ClusterTopologyResult of(Consensus consensus, Set<String> hostIds, Set<CassandraServer> servers) {
            return ImmutableClusterTopologyResult.builder()
                    .consensus(consensus)
                    .hostIds(hostIds)
                    .servers(servers)
                    .build();
        }
    }
}
