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
import com.palantir.atlasdb.keyvalue.cassandra.CassandraTopologyValidator.HostIdResult.Type;
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
import org.immutables.value.Value.Check;

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
            throw new SafeRuntimeException(
                    "Failed to obtain consistent view of hosts from cluster.",
                    e,
                    SafeArg.of("newlyAddedCassandraHosts", newlyAddedHosts),
                    SafeArg.of("allCassandraHosts", allHosts.keySet()));
        } finally {
            metrics.recordTopologyValidationLatency(Duration.ofMillis(stopwatch.elapsed(TimeUnit.MILLISECONDS)));
        }
    }

    /**
     * Checks a set of new Cassandra servers against the current Casssandra servers
     * to ensure their topologies are matching. This is done to prevent user-led split-brain,
     * which can occur if a user accidentally provided hostnames for two different Cassandra clusters.
     *
     * This is done by coming to a consensus (quorum) on the topology of the pre-existing hosts,
     * and then subsequently returning any new hosts which do not match the present topology.
     *
     * Of course, there is the base case of all hosts will be new. In this case, we simply check for
     * consensus on all hosts then, returning only the hosts which did not agree.
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
     * @return Set of Cassandra servers which do not match the pre-existing hosts topology.
     */
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

        // This means we've not added any hosts yet.
        if (currentServers.isEmpty()) {
            return maybeGetConsistentClusterTopology(newServers)
                    .map(topology ->
                            // Do not add new servers which are not in consensus
                            (Set<CassandraServer>) Sets.difference(newlyAddedHosts, topology.serversInConsensus()))
                    .orElse(newlyAddedHosts);
        }

        // If a consensus can be reached from the current servers,
        // filter all new servers which have the same set of host ids.
        // Otherwise, if we cannot come to consensus on the current topology, just refuse to add any new servers.
        return maybeGetConsistentClusterTopology(currentServers)
                .map(topology -> EntryStream.of(fetchHostIdsIgnoringSoftFailures(newServers))
                        .removeValues(hostIds -> hostIds.equals(topology.hostIds()))
                        .keys()
                        .toSet())
                .orElse(newlyAddedHosts);
    }

    /**
     * Obtains a consistent view of the cluster topology for the provided hosts.
     *
     * This is achieved by comparing the hostIds (list of UUIDs for each C* node) for all Cassandra nodes.
     * A quorum of C* nodes are required to be queryable and all nodes queried must have the same value topology
     * for this to return a valid result.
     *
     * @param hosts Cassandra hosts to obtain a consistent topology view from
     * @return If consensus could be reached, the topology and the list of valid hosts, otherwise empty.
     */
    private Optional<ConsistentClusterTopology> maybeGetConsistentClusterTopology(
            Map<CassandraServer, CassandraClientPoolingContainer> hosts) {
        Map<CassandraServer, HostIdResult> hostIdsByServerWithoutSoftFailures = fetchHostIdsIgnoringSoftFailures(hosts);
        if (hostIdsByServerWithoutSoftFailures.isEmpty()) {
            return Optional.empty();
        }

        Map<CassandraServer, Set<String>> hostIdsWithoutHardFailures = EntryStream.of(
                        hostIdsByServerWithoutSoftFailures)
                .removeValues(result -> result.type().equals(Type.HARD_FAILURE))
                .mapValues(HostIdResult::hostIds)
                .toMap();

        int quorum = (hostIdsByServerWithoutSoftFailures.size() + 1) / 2;

        if (hostIdsWithoutHardFailures.size() < quorum) {
            return Optional.empty();
        }

        Set<Set<String>> uniqueSetsOfHostIds =
                EntryStream.of(hostIdsWithoutHardFailures).values().toImmutableSet();

        if (uniqueSetsOfHostIds.size() == 1) {
            Set<String> uniqueHostIds = Iterables.getOnlyElement(uniqueSetsOfHostIds);
            Set<CassandraServer> cassandraServersWithMatchingHostIds = EntryStream.of(
                            hostIdsByServerWithoutSoftFailures)
                    .filterValues(uniqueHostIds::equals)
                    .keys()
                    .toSet();
            Set<CassandraServer> cassandraServerWithoutHostIdEndpoint =
                    Sets.difference(hosts.keySet(), hostIdsByServerWithoutSoftFailures.keySet());
            return Optional.of(ConsistentClusterTopology.builder()
                    .hostIds(uniqueHostIds)
                    .serversInConsensus(
                            Sets.union(cassandraServersWithMatchingHostIds, cassandraServerWithoutHostIdEndpoint))
                    .build());
        }

        return Optional.empty();
    }

    @VisibleForTesting
    HostIdResult fetchHostIds(CassandraClientPoolingContainer container) {
        try {
            return container.<HostIdResult, Exception>runWithPooledResource(client -> HostIdResult.builder()
                    .hostIds(ImmutableSet.copyOf(client.get_host_ids()))
                    .type(Type.SUCCESS)
                    .build());
        } catch (TTransportException e) {
            log.warn(
                    "Failed to get host ids from host due to networking failure.",
                    SafeArg.of("host", container.getCassandraServer().cassandraHostName()),
                    SafeArg.of(
                            "proxy",
                            CassandraLogHelper.host(
                                    container.getCassandraServer().proxy())),
                    e);
            return HostIdResult.hardFailure();
        } catch (Exception e) {
            log.warn(
                    "Failed to get host ids from host due to method not existing on Cassandrda server.",
                    SafeArg.of("host", container.getCassandraServer().cassandraHostName()),
                    SafeArg.of(
                            "proxy",
                            CassandraLogHelper.host(
                                    container.getCassandraServer().proxy())),
                    e);
            return HostIdResult.softFailure();
        }
    }

    private Map<CassandraServer, HostIdResult> fetchHostIdsIgnoringSoftFailures(
            Map<CassandraServer, CassandraClientPoolingContainer> servers) {
        return EntryStream.of(servers)
                .mapValues(this::fetchHostIds)
                .removeValues(result -> Type.SOFT_FAILURE.equals(result.type()))
                .toMap();
    }

    @Value.Immutable
    public interface ConsistentClusterTopology {

        Set<String> hostIds();

        Set<CassandraServer> serversInConsensus();

        static ImmutableConsistentClusterTopology.Builder builder() {
            return ImmutableConsistentClusterTopology.builder();
        }
    }

    @Value.Immutable
    public interface HostIdResult {

        HostIdResult HARD_FAILURE =
                builder().type(Type.HARD_FAILURE).hostIds(Set.of()).build();
        HostIdResult SOFT_FAILURE =
                builder().type(Type.SOFT_FAILURE).hostIds(Set.of()).build();

        enum Type {
            // Failure should affect quorum calculations
            HARD_FAILURE,
            // Failure type shouldn't affect quorum calculations
            SOFT_FAILURE,
            SUCCESS
        }

        Type type();

        Set<String> hostIds();

        static HostIdResult hardFailure() {
            return HARD_FAILURE;
        }

        static HostIdResult softFailure() {
            return SOFT_FAILURE;
        }

        @Check
        default void checkHostIdsStateBasedOnResultType() {
            Preconditions.checkState(
                    !(type().equals(Type.SUCCESS) && hostIds().isEmpty()),
                    "It is expected that there should be at least one host id if the result is successful.");
            Preconditions.checkState(
                    type().equals(Type.SUCCESS) || hostIds().isEmpty(),
                    "It is expected that no hostIds should be present when there is a failure.");
        }

        static ImmutableHostIdResult.Builder builder() {
            return ImmutableHostIdResult.builder();
        }
    }
}
