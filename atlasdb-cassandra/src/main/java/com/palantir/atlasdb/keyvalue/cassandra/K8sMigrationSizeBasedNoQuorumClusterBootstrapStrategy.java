/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

import com.google.common.collect.Iterables;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.CassandraServersConfig;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraTopologyValidator.ClusterTopologyResult;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraTopologyValidator.ConsistentClusterTopology;
import com.palantir.atlasdb.keyvalue.cassandra.pool.CassandraServer;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import one.util.streamex.EntryStream;

public class K8sMigrationSizeBasedNoQuorumClusterBootstrapStrategy implements NoQuorumClusterBootstrapStrategy {
    private static final SafeLogger log =
            SafeLoggerFactory.get(K8sMigrationSizeBasedNoQuorumClusterBootstrapStrategy.class);

    private final Supplier<CassandraServersConfig> runtimeConfigSupplier;

    public K8sMigrationSizeBasedNoQuorumClusterBootstrapStrategy(
            Supplier<CassandraServersConfig> runtimeConfigSupplier) {
        this.runtimeConfigSupplier = runtimeConfigSupplier;
    }

    @Override
    public ClusterTopologyResult accept(Map<CassandraServer, HostIdResult> hostIdResults) {
        if (hostIdResults.values().stream().anyMatch(result -> result.type() == HostIdResult.Type.SOFT_FAILURE)) {
            log.warn(
                    "The size-based no quorum handling strategy was not designed to deal with soft failures."
                            + " Treating as hard failures.",
                    SafeArg.of("hostIdResults", hostIdResults));
        }

        Map<CassandraServer, Set<String>> hostIdsWithoutFailures = HostIdResults.getHostIdsFromSuccesses(hostIdResults);
        if (hostIdsWithoutFailures.isEmpty()) {
            log.warn("With all nodes failing, we cannot determine any candidate topology.");
            return ClusterTopologyResult.noQuorum();
        }
        if (hostIdsWithoutFailures.size() >= HostIdResults.getQuorumSize(hostIdResults.size())) {
            log.warn(
                    "Should not have called a no-quorum handling strategy when a quorum existed; yet, this may"
                            + " occur especially if there were soft failures. This is likely a bug in the topology"
                            + " validator",
                    new SafeRuntimeException("I exist to show you the stack trace"));
        }

        Set<Set<String>> uniqueSetsOfHostIds =
                EntryStream.of(hostIdsWithoutFailures).values().toImmutableSet();
        if (uniqueSetsOfHostIds.size() > 1) {
            log.warn(
                    "Encountered dissent when invoking a no-quorum handling strategy.",
                    SafeArg.of("hostIdResults", hostIdResults));
            return ClusterTopologyResult.dissent();
        }

        Set<CassandraServer> serversInAgreement = hostIdsWithoutFailures.keySet();
        Set<String> uniqueHostIds = Iterables.getOnlyElement(uniqueSetsOfHostIds);

        CassandraServersConfig cassandraServersConfig = runtimeConfigSupplier.get();
        int numberOfConfiguredThriftHosts = cassandraServersConfig.numberOfThriftHosts();

        if (uniqueHostIds.size() == numberOfConfiguredThriftHosts) {
            // This situation may arise after the second cloud is connected, but before its client interfaces are
            // enabled.
            return handleNoQuorumAfterPossibleConnectionOfSecondCloud(
                    serversInAgreement, uniqueHostIds, cassandraServersConfig, numberOfConfiguredThriftHosts);
        } else if (uniqueHostIds.size() == hostIdResults.size()) {
            // This situation may arise after the first cloud's client interfaces are disabled, but before it is
            // decommissioned.
            return handleNoQuorumAfterPossibleDisconnectionOfFirstCloud(
                    hostIdResults, serversInAgreement, uniqueHostIds, cassandraServersConfig);
        } else {
            log.warn(
                    "Less than a quorum of nodes came to a consensus, but rejecting said consensus because the number"
                            + " of host IDs in that Cassandra cluster is neither equal to what was specified in"
                            + " configuration, nor equal to the host ID results we knew about.",
                    SafeArg.of("uniqueHostIds", uniqueHostIds),
                    SafeArg.of("hostIdResults", hostIdResults),
                    SafeArg.of("cassandraServersConfig", cassandraServersConfig),
                    SafeArg.of("numberOfConfiguredThriftHosts", numberOfConfiguredThriftHosts));
            return ClusterTopologyResult.noQuorum();
        }
    }

    private static ClusterTopologyResult handleNoQuorumAfterPossibleConnectionOfSecondCloud(
            Set<CassandraServer> serversInAgreement,
            Set<String> uniqueHostIds,
            CassandraServersConfig cassandraServersConfig,
            int numberOfConfiguredThriftHosts) {
        return checkIfQuorumExistsInSingularCloud(
                numberOfConfiguredThriftHosts,
                "configuration",
                serversInAgreement,
                uniqueHostIds,
                cassandraServersConfig);
    }

    private static ClusterTopologyResult handleNoQuorumAfterPossibleDisconnectionOfFirstCloud(
            Map<CassandraServer, HostIdResult> hostIdResults,
            Set<CassandraServer> serversInAgreement,
            Set<String> uniqueHostIds,
            CassandraServersConfig cassandraServersConfig) {
        return checkIfQuorumExistsInSingularCloud(
                hostIdResults.size(),
                "Cassandra-based discovery",
                serversInAgreement,
                uniqueHostIds,
                cassandraServersConfig);
    }

    private static ClusterTopologyResult checkIfQuorumExistsInSingularCloud(
            int combinedClusterSize,
            String sourceOfClusterSizeBelief,
            Set<CassandraServer> serversInAgreement,
            Set<String> uniqueHostIds,
            CassandraServersConfig cassandraServersConfig) {
        int singleCloudClusterSize = combinedClusterSize / 2;
        if (serversInAgreement.size() < HostIdResults.getQuorumSize(singleCloudClusterSize)) {
            log.warn(
                    "Less than a quorum of nodes came to a consensus, but rejecting said consensus: although the"
                        + " number of host IDs in the Cassandra cluster equals what we believe the cluster size should"
                        + " be, this proposal arose from a minority of nodes that was also insufficient for a quorum"
                        + " in the original cloud, under our migration assumptions.",
                    SafeArg.of("uniqueHostIds", uniqueHostIds),
                    SafeArg.of("serversInAgreement", serversInAgreement),
                    SafeArg.of("cassandraServersConfig", cassandraServersConfig),
                    SafeArg.of("singleCloudClusterSize", singleCloudClusterSize),
                    SafeArg.of("sourceOfClusterSizeBelief", sourceOfClusterSizeBelief));
            return ClusterTopologyResult.noQuorum();
        }

        log.info(
                "Less than a quorum of nodes came to a consensus, but accepting said consensus because the number of"
                    + " host IDs in that Cassandra cluster was equal to what we believe the cluster size should be and"
                    + " the number of agreements constitutes at least a quorum in the original cloud.",
                SafeArg.of("uniqueHostIds", uniqueHostIds),
                SafeArg.of("cassandraServersConfig", cassandraServersConfig),
                SafeArg.of("serversInAgreement", serversInAgreement),
                SafeArg.of("singleCloudClusterSize", singleCloudClusterSize),
                SafeArg.of("sourceOfClusterSizeBelief", sourceOfClusterSizeBelief));
        return ClusterTopologyResult.consensus(ConsistentClusterTopology.builder()
                .hostIds(uniqueHostIds)
                .serversInConsensus(serversInAgreement)
                .build());
    }
}
