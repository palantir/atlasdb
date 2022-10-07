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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.cassandra.pool.CassandraServer;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import one.util.streamex.EntryStream;
import org.apache.thrift.TException;
import org.immutables.value.Value;

public final class ClusterTopologyValidator {
    private static final SafeLogger log = SafeLoggerFactory.get(ClusterTopologyValidator.class);

    private ClusterTopologyValidator() {}

    public static Set<CassandraServer> getNewHostsWithInconsistentTopologies(
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

    private static ClusterTopologyResult getClusterTopology(
            Map<CassandraServer, CassandraClientPoolingContainer> hosts) {
        Map<CassandraServer, Set<String>> fetchHostIdsForServers = fetchHostIdsForServers(hosts);
        if (fetchHostIdsForServers.isEmpty()) {
            return ImmutableUniqueHostIds.of(ClusterTopologyResult.Consensus.HAS_CONSENSUS, Set.of());
        }

        Map<Set<String>, Long> uniqueCountOfHostIdSets = EntryStream.of(fetchHostIdsForServers)
                .values()
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        long expectedCount = (fetchHostIdsForServers.size() + 1) / 2;

        Optional<Set<String>> maybeUniqueHostIds = EntryStream.of(uniqueCountOfHostIdSets)
                .filterValues(count -> count.equals(expectedCount))
                .keys()
                .findFirst();

        return maybeUniqueHostIds
                .map(uniqueHostIds -> ImmutableUniqueHostIds.of(
                        ClusterTopologyResult.Consensus.HAS_CONSENSUS,
                        uniqueHostIds,
                        EntryStream.of(fetchHostIdsForServers)
                                .filterValues(hostIds -> hostIds.equals(uniqueHostIds))
                                .keys()
                                .toSet()))
                .orElseGet(() ->
                        ImmutableUniqueHostIds.of(ClusterTopologyResult.Consensus.NO_CONSENSUS, Set.of(), Set.of()));
    }

    private static Map<CassandraServer, Set<String>> fetchHostIdsForServers(
            Map<CassandraServer, CassandraClientPoolingContainer> servers) {
        return EntryStream.of(servers)
                .mapValues(ClusterTopologyValidator::fetchHostIds)
                .removeValues(Optional::isEmpty)
                .mapValues(Optional::get)
                .toMap();
    }

    private static Optional<Set<String>> fetchHostIds(CassandraClientPoolingContainer container) {
        return container.<Optional<Set<String>>>runWithPooledResource(client -> {
            try {
                return Optional.of(ImmutableSet.copyOf(client.get_host_ids()));
            } catch (TException e) {
                log.warn(
                        "Failed to get host ids from host",
                        SafeArg.of("host", container.getCassandraServer().cassandraHostName()),
                        SafeArg.of(
                                "proxy",
                                CassandraLogHelper.host(
                                        container.getCassandraServer().proxy())),
                        e);
                return Optional.of(ImmutableSet.of());
            }
        });
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
    }
}
