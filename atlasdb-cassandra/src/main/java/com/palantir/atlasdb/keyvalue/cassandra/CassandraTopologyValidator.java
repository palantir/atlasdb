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
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.CassandraTopologyValidationMetrics;
import com.palantir.atlasdb.keyvalue.cassandra.pool.CassandraServer;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import one.util.streamex.EntryStream;
import org.apache.thrift.TApplicationException;
import org.immutables.value.Value;

public final class CassandraTopologyValidator {
    private static final SafeLogger log = SafeLoggerFactory.get(CassandraTopologyValidator.class);
    private final CassandraTopologyValidationMetrics metrics;

    public CassandraTopologyValidator(CassandraTopologyValidationMetrics metrics) {
        this.metrics = metrics;
    }

    /**
     * Checks a set of new Cassandra servers against the current Casssandra servers
     * to ensure their topologies are matching. This is done to prevent user-led split-brain,
     * which can occur if a user accidentally provided hostnames for two different Cassandra clusters.
     *
     * This is done by coming to a consensus on the topology of the pre-existing hosts,
     * and then subsequently returning any new hosts which do not match the present topology.
     *
     * Of course, there is the base case of all hosts will be new. In this case, we simply check that all
     * new hosts are in consensus.
     *
     * Servers that do not have support for the get_host_ids endpoint are always considered consistent,
     * even if we cannot come to a consensus on the hosts that do support the endpoint.
     *
     * Consensus is defined as:
     * (1) A quorum of nodes (excluding those without `get_host_ids` support) are reachable.
     * (2) All reachable nodes have the same set of hostIds.
     * (3) All Cassandra nodes without get_host_ids support are considered to be matching.
     *
     * The above should be sufficient to prevent user-led split-brain as:
     * (1) The initial list of servers validate that they've at least quorum for consensus of topology.
     * (2) All new hosts added then must match the set of pre-existing hosts topology.
     *
     * There does exist an edge case of, two sets of Cassandra clusters being added (3 and 6 respectively).
     * On initialization, the Cassandra cluster with 6 will be used as the base case if the other 3 nodes
     * are down, as this will satisfy quorum requirements. However, the cluster of 6 could be the wrong
     * cluster, which means we're reading/writing from the wrong cluster! However, this then requires we
     * check all nodes, which then means we cannot handle Cassandra restarts, thus this is the best we can do.
     *
     * @param newlyAddedHosts Set of new Cassandra servers you wish to validate.
     * @param allHosts All Cassandra servers which must include newlyAddedHosts.
     * @return Set of Cassandra servers which do not match the pre-existing hosts topology. Servers without
     * the get_host_ids endpoint will never be returned here.
     */
    public Set<CassandraServer> getNewHostsWithInconsistentTopologiesAndRetry(
            Set<CassandraServer> newlyAddedHosts,
            Map<CassandraServer, CassandraClientPoolingContainer> allHosts,
            Duration waitTimeBetweenCalls,
            Duration maxWaitTime) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        Retryer<Set<CassandraServer>> retryer = RetryerBuilder.<Set<CassandraServer>>newBuilder()
                .retryIfResult(servers -> servers.size() == allHosts.size())
                .retryIfException()
                .withWaitStrategy(WaitStrategies.fixedWait(waitTimeBetweenCalls.toMillis(), TimeUnit.MILLISECONDS))
                .withStopStrategy(StopStrategies.stopAfterDelay(maxWaitTime.toMillis(), TimeUnit.MILLISECONDS))
                .build();
        Supplier<Set<CassandraServer>> inconsistentNewHosts =
                () -> getNewHostsWithInconsistentTopologies(newlyAddedHosts, allHosts);
        try {
            return retryer.call(inconsistentNewHosts::get);
        } catch (RetryException | ExecutionException e) {
            metrics.validationFailures().inc();
            log.error(
                    "Failed to obtain consistent view of hosts from cluster.",
                    SafeArg.of("newlyAddedCassandraHosts", newlyAddedHosts),
                    SafeArg.of("allCassandraHosts", allHosts.keySet()),
                    e);
            return inconsistentNewHosts.get();
        } finally {
            metrics.validationLatency().update(stopwatch.elapsed(TimeUnit.MILLISECONDS));
        }
    }

    @VisibleForTesting
    Set<CassandraServer> getNewHostsWithInconsistentTopologies(
            Set<CassandraServer> newlyAddedHosts, Map<CassandraServer, CassandraClientPoolingContainer> allHosts) {

        if (newlyAddedHosts.isEmpty()) {
            return newlyAddedHosts;
        }

        Preconditions.checkArgument(
                allHosts.keySet().containsAll(newlyAddedHosts),
                "Newly added hosts must be a subset of all hosts, as otherwise we have no way to query them.",
                SafeArg.of("newlyAddedHosts", CassandraLogHelper.collectionOfHosts(newlyAddedHosts)),
                SafeArg.of("allHosts", CassandraLogHelper.collectionOfHosts(allHosts.keySet())));

        Map<CassandraServer, HostIdResult> hostIdsByServerWithoutSoftFailures =
                fetchHostIdsIgnoringSoftFailures(allHosts);

        Map<CassandraServer, HostIdResult> currentServersWithoutSoftFailures = EntryStream.of(
                        hostIdsByServerWithoutSoftFailures)
                .removeKeys(newlyAddedHosts::contains)
                .toMap();
        Map<CassandraServer, HostIdResult> newServersWithoutSoftFailures = EntryStream.of(
                        hostIdsByServerWithoutSoftFailures)
                .filterKeys(newlyAddedHosts::contains)
                .toMap();

        // This means currently we've no servers or no server without the get_host_ids endpoint.
        // Therefore, we need to come to a consensus on the new servers.
        if (currentServersWithoutSoftFailures.isEmpty()) {
            return maybeGetConsistentClusterTopology(newServersWithoutSoftFailures)
                    .<Set<CassandraServer>>map(topology ->
                            // Do not add new servers which were unreachable
                            Sets.difference(newServersWithoutSoftFailures.keySet(), topology.serversInConsensus()))
                    .orElseGet(newServersWithoutSoftFailures::keySet);
        }

        // If a consensus can be reached from the current servers,
        // filter all new servers which have the same set of host ids.
        // Otherwise, if we cannot come to consensus on the current topology, just refuse to add any new servers.
        return maybeGetConsistentClusterTopology(currentServersWithoutSoftFailures)
                .map(topology -> EntryStream.of(newServersWithoutSoftFailures)
                        .removeValues(result -> result.type() == HostIdResult.Type.SUCCESS
                                && result.hostIds().equals(topology.hostIds()))
                        .keys()
                        .toSet())
                .orElseGet(newServersWithoutSoftFailures::keySet);
    }

    /**
     * Obtains a consistent view of the cluster topology for the provided hosts.
     *
     * This is achieved by comparing the hostIds (list of UUIDs for each C* node) for all Cassandra nodes.
     * A quorum of C* nodes are required to be reachable and all reachable nodes must have the same
     * topology (hostIds) for this to return a valid result. Nodes that are reachable but do not have
     * support for our get_host_ids endpoint are simply ignored and will not be filtered out.
     *
     * @param hostIdsByServerWithoutSoftFailures Cassandra hosts to obtain a consistent topology view from
     * @return If consensus could be reached, the topology and the list of valid hosts, otherwise empty.
     */
    private Optional<ConsistentClusterTopology> maybeGetConsistentClusterTopology(
            Map<CassandraServer, HostIdResult> hostIdsByServerWithoutSoftFailures) {
        log.info("Trying to get a consistent cluster topology",
                SafeArg.of("results", hostIdsByServerWithoutSoftFailures));
        // If all our queries fail due to soft failures, then our consensus is an empty set of host ids
        if (hostIdsByServerWithoutSoftFailures.isEmpty()) {
            return Optional.of(ConsistentClusterTopology.builder()
                    .hostIds(Set.of())
                    .serversInConsensus(hostIdsByServerWithoutSoftFailures.keySet())
                    .build());
        }

        Map<CassandraServer, Set<String>> hostIdsWithoutFailures = EntryStream.of(hostIdsByServerWithoutSoftFailures)
                .filterValues(result -> result.type() == HostIdResult.Type.SUCCESS)
                .mapValues(HostIdResult::hostIds)
                .toMap();

        // Only consider hosts that have the endpoint for quorum calculations.
        // Otherwise, we will never add hosts when we're in a mixed state
        int quorum = (hostIdsByServerWithoutSoftFailures.size() + 1) / 2;

        // If too many hosts are unreachable, then we cannot come to a consensus
        if (hostIdsWithoutFailures.size() < quorum) {
            return Optional.empty();
        }

        Set<Set<String>> uniqueSetsOfHostIds =
                EntryStream.of(hostIdsWithoutFailures).values().toImmutableSet();

        // If we only have one set of host ids, we've consensus, otherwise fail
        if (uniqueSetsOfHostIds.size() == 1) {
            Set<String> uniqueHostIds = Iterables.getOnlyElement(uniqueSetsOfHostIds);
            return Optional.of(ConsistentClusterTopology.builder()
                    .hostIds(uniqueHostIds)
                    .serversInConsensus(hostIdsWithoutFailures.keySet())
                    .build());
        }

        return Optional.empty();
    }

    private Map<CassandraServer, HostIdResult> fetchHostIdsIgnoringSoftFailures(
            Map<CassandraServer, CassandraClientPoolingContainer> servers) {
        log.info("Attempting to fetch host IDs, ignoring soft failures",
                SafeArg.of("servers", servers.keySet()));
        Map<CassandraServer, HostIdResult> results =
                EntryStream.of(servers).mapValues(this::fetchHostIds).toMap();

        if (KeyedStream.stream(results)
                .values()
                .anyMatch(result -> result.type() == HostIdResult.Type.SOFT_FAILURE
                        || result.type() == HostIdResult.Type.HARD_FAILURE)) {
            log.warn(
                    "While fetching host id from hosts, some reported soft and hard failures.",
                    SafeArg.of("results", results));
        } else {
            log.info("Looks like host ids were ok",
                    SafeArg.of("results", results));
        }

        return EntryStream.of(results)
                .removeValues(result -> result.type() == HostIdResult.Type.SOFT_FAILURE)
                .toMap();
    }

    @VisibleForTesting
    HostIdResult fetchHostIds(CassandraClientPoolingContainer container) {
        try {
            return container.<HostIdResult, Exception>runWithPooledResource(
                    client -> HostIdResult.success(client.get_host_ids()));
        } catch (Exception e) {
            // If the get_host_ids API endpoint does not exist, then return a soft failure.
            if (e instanceof TApplicationException) {
                TApplicationException applicationException = (TApplicationException) e;
                if (applicationException.getType() == TApplicationException.UNKNOWN_METHOD) {
                    return HostIdResult.softFailure();
                }
            }
            return HostIdResult.hardFailure();
        }
    }

    @Value.Immutable
    public interface ConsistentClusterTopology {

        Set<String> hostIds();

        Set<CassandraServer> serversInConsensus();

        static ImmutableConsistentClusterTopology.Builder builder() {
            return ImmutableConsistentClusterTopology.builder();
        }
    }
}
