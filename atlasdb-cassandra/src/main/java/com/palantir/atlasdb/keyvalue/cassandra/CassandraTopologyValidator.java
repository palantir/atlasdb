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
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import one.util.streamex.EntryStream;
import org.apache.thrift.TApplicationException;
import org.immutables.value.Value;

public final class CassandraTopologyValidator {
    private static final SafeLogger log = SafeLoggerFactory.get(CassandraTopologyValidator.class);
    private final CassandraTopologyValidationMetrics metrics;
    private final AtomicReference<ConsistentClusterTopology> pastConsistentTopology;

    public CassandraTopologyValidator(CassandraTopologyValidationMetrics metrics) {
        this.metrics = metrics;
        this.pastConsistentTopology = new AtomicReference<>();
    }

    /**
     * Checks a set of new Cassandra servers against the current Casssandra servers
     * to ensure their topologies are matching. This is done to prevent user-led split-brain,
     * which can occur if a user accidentally provided hostnames for two different Cassandra clusters.
     * <p>
     * This is done by coming to a consensus on the topology of the pre-existing hosts,
     * and then subsequently returning any new hosts which do not match the present topology.
     * <p>
     * Of course, there is the base case of all hosts will be new. In this case, we simply check that all
     * new hosts are in consensus.
     * <p>
     * Servers that do not have support for the get_host_ids endpoint are always considered consistent,
     * even if we cannot come to a consensus on the hosts that do support the endpoint.
     * <p>
     * Consensus may be demonstrated independently by a set of nodes. In this case, we require that:
     * (1) A quorum of nodes (excluding those without `get_host_ids` support) are reachable.
     * (2) All reachable nodes have the same set of hostIds.
     * (3) All Cassandra nodes without get_host_ids support are considered to be matching.
     * <p>
     * The above should be sufficient to prevent user-led split-brain as:
     * (1) The initial list of servers validate that they've at least quorum for consensus of topology.
     * (2) All new hosts added then must match the set of pre-existing hosts topology.
     * <p>
     * Consensus may also be demonstrated and new hosts added without a quorum of nodes being reachable, if:
     * (4) New hosts support get_host_ids, and have the same set of hostIds as the most recent previous consensus
     * satisfied through conditions (1) - (3).
     * <p>
     * In this case, we know that a previous set of servers had quorum for a consensus, which we are also agreeing to.
     * Since we aren't agreeing on any new values, values that were agreed upon must have passed conditions (1) - (3)
     * at the time of their inception, and that required a quorum of nodes to agree.
     * <p>
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
            Map<CassandraServer, CassandraServerOrigin> newlyAddedHosts,
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
            Map<CassandraServer, CassandraServerOrigin> newlyAddedHosts,
            Map<CassandraServer, CassandraClientPoolingContainer> allHosts) {

        Set<CassandraServer> newlyAddedHostsWithoutOrigin = newlyAddedHosts.keySet();
        if (newlyAddedHosts.isEmpty()) {
            return newlyAddedHostsWithoutOrigin;
        }

        Preconditions.checkArgument(
                allHosts.keySet().containsAll(newlyAddedHostsWithoutOrigin),
                "Newly added hosts must be a subset of all hosts, as otherwise we have no way to query them.",
                SafeArg.of("newlyAddedHosts", CassandraLogHelper.collectionOfHosts(newlyAddedHostsWithoutOrigin)),
                SafeArg.of("allHosts", CassandraLogHelper.collectionOfHosts(allHosts.keySet())));

        Map<CassandraServer, HostIdResult> hostIdsByServerWithoutSoftFailures =
                fetchHostIdsIgnoringSoftFailures(allHosts);

        Map<CassandraServer, HostIdResult> currentServersWithoutSoftFailures = EntryStream.of(
                        hostIdsByServerWithoutSoftFailures)
                .removeKeys(newlyAddedHosts::containsKey)
                .toMap();
        Map<CassandraServer, HostIdResult> newServersWithoutSoftFailures = EntryStream.of(
                        hostIdsByServerWithoutSoftFailures)
                .filterKeys(newlyAddedHosts::containsKey)
                .toMap();

        // This means currently we've no servers or no server without the get_host_ids endpoint.
        // Therefore, we need to come to a consensus on the new servers.
        if (currentServersWithoutSoftFailures.isEmpty()) {
            log.info(
                    "Case one. No current servers, so we need to come to a consensus on the new servers.",
                    SafeArg.of("newlyAddedHosts", newlyAddedHosts),
                    SafeArg.of("allHosts", allHosts));
            ClusterTopologyResult topologyResultFromNewServers =
                    maybeGetConsistentClusterTopology(newServersWithoutSoftFailures);
            log.info(
                    "Case one. Topology is as follows.",
                    SafeArg.of("newlyAddedHosts", newlyAddedHosts),
                    SafeArg.of("allHosts", allHosts),
                    SafeArg.of("topologyResult", topologyResultFromNewServers));
            Map<CassandraServer, HostIdResult> newServersFromConfig = EntryStream.of(newServersWithoutSoftFailures)
                    .filterKeys(server -> newlyAddedHosts.get(server) == CassandraServerOrigin.CONFIG)
                    .toMap(); // this can be empty
            if (newServersFromConfig.isEmpty()) {
                // We've no servers. But none of our servers came from config - ?!
                // This should never happen, but if it does, it's unsafe to do everything
                log.info(
                        "We have no servers, but somehow got a token ring. This is unexpected. To recover, we will "
                                + "return all new servers as inconsistent.",
                        SafeArg.of("newlyAddedHosts", newlyAddedHosts),
                        SafeArg.of("allHosts", allHosts),
                        SafeArg.of("topologyResult", topologyResultFromNewServers));
                return getNewHostsWithInconsistentTopologiesFromTopologyResult(
                        topologyResultFromNewServers,
                        newServersWithoutSoftFailures,
                        newServersWithoutSoftFailures,
                        newlyAddedHostsWithoutOrigin,
                        allHosts.keySet());
            }
            return getNewHostsWithInconsistentTopologiesFromTopologyResult(
                    topologyResultFromNewServers,
                    newServersWithoutSoftFailures,
                    newServersFromConfig,
                    newlyAddedHostsWithoutOrigin,
                    allHosts.keySet());
        }

        // If a consensus can be reached from the current servers, filter all new servers which have the same set of
        // host ids. Accept dissent as such, but permit new servers if they are in quorum _and_ match the previously
        // accepted set of host IDs
        ClusterTopologyResult topologyFromCurrentServers =
                maybeGetConsistentClusterTopology(currentServersWithoutSoftFailures);
        log.info(
                "Case two. Topology is as follows.",
                SafeArg.of("newlyAddedHosts", newlyAddedHosts),
                SafeArg.of("allHosts", CassandraLogHelper.collectionOfHosts(allHosts.keySet())),
                SafeArg.of("topologyResult", topologyFromCurrentServers));

        return getNewHostsWithInconsistentTopologiesFromTopologyResult(
                topologyFromCurrentServers,
                newServersWithoutSoftFailures,
                newServersWithoutSoftFailures,
                newlyAddedHosts.keySet(),
                allHosts.keySet());
    }

    private Set<CassandraServer> getNewHostsWithInconsistentTopologiesFromTopologyResult(
            ClusterTopologyResult topologyResult,
            Map<CassandraServer, HostIdResult> newServersWithoutSoftFailures,
            Map<CassandraServer, HostIdResult> serversToConsiderWhenNoQuorumPresent,
            Set<CassandraServer> newlyAddedHosts,
            Set<CassandraServer> allHosts) {
        switch (topologyResult.type()) {
            case CONSENSUS:
                Preconditions.checkState(
                        topologyResult.agreedTopology().isPresent(),
                        "Expected to have a consistent topology for a CONSENSUS result, but did not.");
                ConsistentClusterTopology topology =
                        topologyResult.agreedTopology().get();
                pastConsistentTopology.set(topology);
                return EntryStream.of(newServersWithoutSoftFailures)
                        .removeValues(result -> result.type() == HostIdResult.Type.SUCCESS
                                && result.hostIds().equals(topology.hostIds()))
                        .keys()
                        .toSet();
            case DISSENT:
                // In the event of *active* dissent, we want to hard fail.
                return newServersWithoutSoftFailures.keySet();
            case NO_QUORUM:
                // In the event of no quorum, we trust the new servers iff they agree with our historical knowledge
                // of what the old servers were thinking, since in containerised deployments all nodes can change
                // between refreshes for legitimate reasons (but they should still refer to the same underlying
                // cluster).
                if (pastConsistentTopology.get() == null) {
                    log.warn(
                            "No quorum observed, and we weren't able to get a past consistent topology. Using"
                                    + " configuration information in an attempt to retrieve a consistent topology.",
                            SafeArg.of(
                                    "serversToConsiderWhenNoQuorumPresent",
                                    CassandraLogHelper.collectionOfHosts(
                                            serversToConsiderWhenNoQuorumPresent.keySet())),
                            SafeArg.of("allServers", CassandraLogHelper.collectionOfHosts(allHosts)));
                    ClusterTopologyResult result =
                            maybeGetConsistentClusterTopology(serversToConsiderWhenNoQuorumPresent);
                    if (result.agreedTopology().isPresent()) {
                        pastConsistentTopology.set(result.agreedTopology().get());
                        return Sets.difference(
                                newServersWithoutSoftFailures.keySet(),
                                result.agreedTopology().get().serversInConsensus());
                    }

                    // We don't have quorum with servers from config nor previous topology.
                    return newServersWithoutSoftFailures.keySet();
                }
                Optional<ConsistentClusterTopology> maybeTopology = maybeGetConsistentClusterTopology(
                                serversToConsiderWhenNoQuorumPresent)
                        .agreedTopology();
                if (maybeTopology.isEmpty()) {
                    log.info(
                            "No quorum was detected in original set of servers, and the filtered set of servers were"
                                    + " also not in agreement. Not adding new servers in this case.",
                            SafeArg.of("newServers", CassandraLogHelper.collectionOfHosts(newlyAddedHosts)),
                            SafeArg.of("allServers", CassandraLogHelper.collectionOfHosts(allHosts)),
                            SafeArg.of("filteredServers", serversToConsiderWhenNoQuorumPresent.keySet()));
                    return newServersWithoutSoftFailures.keySet();
                }
                ConsistentClusterTopology newNodesAgreedTopology = maybeTopology.get();
                if (!newNodesAgreedTopology
                        .hostIds()
                        .equals(pastConsistentTopology.get().hostIds())) {
                    log.info(
                            "No quorum was detected among the original set of servers. While a filtered set of"
                                    + " servers could reach a consensus, this differed from the last agreed value"
                                    + " among the old servers. Not adding new servers in this case.",
                            SafeArg.of("pastConsistentTopology", pastConsistentTopology.get()),
                            SafeArg.of("newNodesAgreedTopology", newNodesAgreedTopology), // Agreed
                            SafeArg.of("newServers", CassandraLogHelper.collectionOfHosts(newlyAddedHosts)),
                            SafeArg.of("allServers", CassandraLogHelper.collectionOfHosts(allHosts)),
                            SafeArg.of("filteredServers", serversToConsiderWhenNoQuorumPresent.keySet()));
                    return newServersWithoutSoftFailures.keySet();
                }
                log.info(
                        "No quorum was detected among the original set of servers. A filtered set of servers reached"
                                + " a consensus that matched the last agreed value among the old servers. Adding new"
                                + " servers that were in consensus.",
                        SafeArg.of("pastConsistentTopology", pastConsistentTopology.get()),
                        SafeArg.of("newNodesAgreedTopology", newNodesAgreedTopology),
                        SafeArg.of("newServers", CassandraLogHelper.collectionOfHosts(newlyAddedHosts)),
                        SafeArg.of("allServers", CassandraLogHelper.collectionOfHosts(allHosts)),
                        SafeArg.of("filteredServers", serversToConsiderWhenNoQuorumPresent.keySet()));
                // JKONG: Bug here, though not like this matters
                pastConsistentTopology.getAndUpdate(pastTopology -> ImmutableConsistentClusterTopology.builder()
                        .from(pastTopology)
                        .addAllServersInConsensus(newNodesAgreedTopology.serversInConsensus())
                        .build());
                return Sets.difference(
                        newServersWithoutSoftFailures.keySet(), newNodesAgreedTopology.serversInConsensus());
            default:
                throw new SafeIllegalStateException(
                        "Unexpected cluster topology result type", SafeArg.of("type", topologyResult.type()));
        }
    }

    /**
     * Obtains a consistent view of the cluster topology for the provided hosts.
     * <p>
     * This is achieved by comparing the hostIds (list of UUIDs for each C* node) for all Cassandra nodes.
     * A quorum of C* nodes are required to be reachable and all reachable nodes must have the same
     * topology (hostIds) for this to return a valid result. Nodes that are reachable but do not have
     * support for our get_host_ids endpoint are simply ignored and will not be filtered out.
     *
     * @param hostIdsByServerWithoutSoftFailures Cassandra hosts to obtain a consistent topology view from
     * @return If consensus could be reached, the topology and the list of valid hosts, otherwise empty.
     */
    private ClusterTopologyResult maybeGetConsistentClusterTopology(
            Map<CassandraServer, HostIdResult> hostIdsByServerWithoutSoftFailures) {
        // If all our queries fail due to soft failures, then our consensus is an empty set of host ids
        if (hostIdsByServerWithoutSoftFailures.isEmpty()) {
            log.info("Was asked to return a cluster topology from 0 nodes.");
            return ClusterTopologyResult.consensus(ConsistentClusterTopology.builder()
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
        int quorum = (hostIdsByServerWithoutSoftFailures.size() / 2) + 1;

        // If too many hosts are unreachable, then we cannot come to a consensus
        if (hostIdsWithoutFailures.size() < quorum) {
            log.info(
                    "No quorum topology",
                    SafeArg.of("hostIdsByServerWithoutSoftFailures", hostIdsByServerWithoutSoftFailures));
            return ClusterTopologyResult.noQuorum();
        }

        Set<Set<String>> uniqueSetsOfHostIds =
                EntryStream.of(hostIdsWithoutFailures).values().toImmutableSet();

        // If we only have one set of host ids, we've consensus, otherwise fail
        if (uniqueSetsOfHostIds.size() == 1) {
            Set<String> uniqueHostIds = Iterables.getOnlyElement(uniqueSetsOfHostIds);
            ClusterTopologyResult topology = ClusterTopologyResult.consensus(ConsistentClusterTopology.builder()
                    .hostIds(uniqueHostIds)
                    .serversInConsensus(hostIdsWithoutFailures.keySet())
                    .build());
            log.info(
                    "Achieved topology",
                    SafeArg.of("topology", topology),
                    SafeArg.of("hostIdsByServerWithoutSoftFailures", hostIdsByServerWithoutSoftFailures));
            return topology;
        }

        log.info(
                "Dissenting topology",
                SafeArg.of("hostIdsByServerWithoutSoftFailures", hostIdsByServerWithoutSoftFailures));
        return ClusterTopologyResult.dissent();
    }

    private Map<CassandraServer, HostIdResult> fetchHostIdsIgnoringSoftFailures(
            Map<CassandraServer, CassandraClientPoolingContainer> servers) {
        Map<CassandraServer, HostIdResult> results =
                EntryStream.of(servers).mapToValue(this::fetchHostIds).toMap();

        if (KeyedStream.stream(results)
                .values()
                .anyMatch(result -> result.type() == HostIdResult.Type.SOFT_FAILURE
                        || result.type() == HostIdResult.Type.HARD_FAILURE)) {
            log.warn(
                    "While fetching host id from hosts, some reported soft and hard failures.",
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
            log.info("Hard failure talking to Cassandra", SafeArg.of("container", container), e);
            return HostIdResult.hardFailure();
        }
    }

    @VisibleForTesting
    HostIdResult fetchHostIds(CassandraServer server, CassandraClientPoolingContainer container) {
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
            log.info("Hard failure talking to Cassandra", SafeArg.of("cassandraServer", server), e);
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

    enum ClusterTopologyResultType {
        CONSENSUS,
        DISSENT,
        NO_QUORUM
    }

    @Value.Immutable
    public interface ClusterTopologyResult {
        ClusterTopologyResultType type();

        Optional<ConsistentClusterTopology> agreedTopology();

        static ClusterTopologyResult consensus(ConsistentClusterTopology consistentClusterTopology) {
            return ImmutableClusterTopologyResult.builder()
                    .type(ClusterTopologyResultType.CONSENSUS)
                    .agreedTopology(consistentClusterTopology)
                    .build();
        }

        static ClusterTopologyResult dissent() {
            return ImmutableClusterTopologyResult.builder()
                    .type(ClusterTopologyResultType.DISSENT)
                    .build();
        }

        static ClusterTopologyResult noQuorum() {
            return ImmutableClusterTopologyResult.builder()
                    .type(ClusterTopologyResultType.NO_QUORUM)
                    .build();
        }
    }
}
