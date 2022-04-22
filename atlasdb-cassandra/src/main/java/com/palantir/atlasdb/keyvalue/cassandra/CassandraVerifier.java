/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Verify;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.CassandraServersConfig;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.ThriftHostsExtractingVisitor;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.base.Throwables;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.cassandra.thrift.EndpointDetails;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.commons.lang3.Validate;
import org.apache.thrift.TException;
import org.immutables.value.Value;

public final class CassandraVerifier {
    private static final SafeLogger log = SafeLoggerFactory.get(CassandraVerifier.class);
    private static final KsDef SIMPLE_RF_TEST_KS_DEF = new KsDef(
                    CassandraConstants.SIMPLE_RF_TEST_KEYSPACE, CassandraConstants.SIMPLE_STRATEGY, ImmutableList.of())
            .setStrategy_options(ImmutableMap.of(CassandraConstants.REPLICATION_FACTOR_OPTION, "1"));
    private static final String SIMPLE_PARTITIONING_ERROR_MSG = "This cassandra cluster is running using the simple "
            + "partitioning strategy. This partitioner is not rack aware and is not intended for use on prod. "
            + "This will have to be fixed by manually configuring to the network partitioner "
            + "and running the appropriate repairs; talking to support first is recommended. "
            + "If you're running in some sort of environment where nodes have no known correlated "
            + "failure patterns, you can set the 'ignoreNodeTopologyChecks' KVS config option.";

    @VisibleForTesting
    static final Cache<CassandraServersConfig, Set<String>> sanityCheckedDatacenters =
            Caffeine.newBuilder().expireAfterWrite(Duration.ofSeconds(60)).build();

    private CassandraVerifier() {
        // Utility class
    }

    static final FunctionCheckedException<CassandraClient, Void, Exception> healthCheck = client -> {
        client.describe_version();
        return null;
    };

    static Set<String> sanityCheckDatacenters(
            CassandraClient client,
            CassandraServersConfig cassandraServersConfig,
            int replicationFactor,
            boolean ignoreNodeTopologyChecks) {
        return sanityCheckedDatacenters.get(cassandraServersConfig, _kvsConfig -> {
            try {
                return sanityCheckDatacentersInternal(client, replicationFactor, ignoreNodeTopologyChecks);
            } catch (TException e) {
                throw Throwables.throwUncheckedException(e);
            }
        });
    }

    private static Set<String> sanityCheckDatacentersInternal(
            CassandraClient client, int replicationFactor, boolean ignoreNodeTopologyChecks) throws TException {
        createSimpleRfTestKeyspaceIfNotExists(client);

        Multimap<String, String> datacenterToRack = HashMultimap.create();
        Set<String> hosts = new HashSet<>();
        for (TokenRange tokenRange : client.describe_ring(CassandraConstants.SIMPLE_RF_TEST_KEYSPACE)) {
            for (EndpointDetails details : tokenRange.getEndpoint_details()) {
                datacenterToRack.put(details.datacenter, details.rack);
                hosts.add(details.host);
            }
        }

        if (clusterHasExactlyOneDatacenter(datacenterToRack) && replicationFactor > 1) {
            checkNodeTopologyIsSet(datacenterToRack, ignoreNodeTopologyChecks);
            checkMoreRacksThanRfOrFewerHostsThanRf(
                    hosts, replicationFactor, datacenterToRack, ignoreNodeTopologyChecks);
        }

        return datacenterToRack.keySet();
    }

    private static void createSimpleRfTestKeyspaceIfNotExists(CassandraClient client) throws TException {
        if (client.describe_keyspaces().stream()
                .map(KsDef::getName)
                .noneMatch(keyspace -> Objects.equals(keyspace, CassandraConstants.SIMPLE_RF_TEST_KEYSPACE))) {
            client.system_add_keyspace(SIMPLE_RF_TEST_KS_DEF);
        }
    }

    private static boolean clusterHasExactlyOneDatacenter(Multimap<String, String> datacenterToRack) {
        return datacenterToRack.keySet().size() == 1;
    }

    private static boolean clusterHasExactlyOneRack(Multimap<String, String> datacenterToRack) {
        return datacenterToRack.values().size() == 1;
    }

    private static void checkMoreRacksThanRfOrFewerHostsThanRf(
            Set<String> hosts,
            int replicationFactor,
            Multimap<String, String> dcRack,
            boolean ignoreNodeTopologyChecks) {
        if (dcRack.values().size() < replicationFactor && hosts.size() > replicationFactor) {
            logErrorOrThrow(
                    "The cassandra cluster only has one DC, "
                            + "and is set up with less racks than the desired number of replicas, "
                            + "and there are more hosts than the replication factor. "
                            + "It is very likely that your rack configuration is incorrect and replicas "
                            + "would not be placed correctly for the failure tolerance you want. "
                            + "If you fully understand how NetworkTopology replica placement strategy will be placing "
                            + "your replicas, feel free to set the 'ignoreNodeTopologyChecks' KVS config option.",
                    ignoreNodeTopologyChecks);
        }
    }

    private static void checkNodeTopologyIsSet(Multimap<String, String> dcRack, boolean ignoreNodeTopologyChecks) {
        if (clusterHasExactlyOneRack(dcRack)) {
            String datacenter = Iterables.getOnlyElement(dcRack.keySet());
            String rack = Iterables.getOnlyElement(dcRack.values());
            if (datacenter.equals(CassandraConstants.DEFAULT_DC) && rack.equals(CassandraConstants.DEFAULT_RACK)) {
                logErrorOrThrow(
                        "The cassandra cluster is not set up to be datacenter and rack aware. Please set up "
                                + "Cassandra to use NetworkTopology and add corresponding snitch information "
                                + "before running with a replication factor higher than 1. "
                                + "If you're running in some sort of environment where nodes have no known correlated "
                                + "failure patterns, you can set the 'ignoreNodeTopologyChecks' KVS config option.",
                        ignoreNodeTopologyChecks);
            }
        }
    }

    @SuppressWarnings("ValidateConstantMessage") // https://github.com/palantir/gradle-baseline/pull/175
    static void sanityCheckTableName(TableReference tableRef) {
        String tableName = tableRef.getQualifiedName();
        Validate.isTrue(
                !(tableName.startsWith("_") && tableName.contains("."))
                        || AtlasDbConstants.HIDDEN_TABLES.contains(tableRef)
                        || tableName.startsWith(AtlasDbConstants.NAMESPACE_PREFIX),
                "invalid tableName: %s",
                tableName);
    }

    /**
     * If {@code shouldLogAndNotThrow} is set, log the {@code errorMessage} at error.
     * The {@code errorMessage} is considered safe to be logged.
     * If {@code shouldLogAndNotThrow} is not set, an IllegalStateException is thrown with message {@code errorMessage}.
     */
    static void logErrorOrThrow(String errorMessage, boolean shouldLogAndNotThrow) {
        if (shouldLogAndNotThrow) {
            log.error(
                    "{} This would have normally resulted in Palantir exiting however "
                            + "safety checks have been disabled.",
                    SafeArg.of("errorMessage", errorMessage));
        } else {
            throw new IllegalStateException(errorMessage);
        }
    }

    static void validatePartitioner(String partitioner, CassandraKeyValueServiceConfig config) {
        if (!config.ignorePartitionerChecks()) {
            Verify.verify(
                    CassandraConstants.ALLOWED_PARTITIONERS.contains(partitioner),
                    "Invalid partitioner. Allowed: %s, but partitioner is: %s. "
                            + "If you fully understand how it will affect range scan performance and vnode generation, "
                            + "you can set 'ignorePartitionerChecks' in your config.",
                    CassandraConstants.ALLOWED_PARTITIONERS,
                    partitioner);
        }
    }

    static void ensureKeyspaceExistsAndIsUpToDate(
            CassandraClientPool clientPool, CassandraKeyspaceConfig cassandraKeyspaceConfig) throws TException {
        createKeyspace(cassandraKeyspaceConfig);
        updateExistingKeyspace(clientPool, cassandraKeyspaceConfig);
    }

    private static void createKeyspace(CassandraKeyspaceConfig cassandraKeyspaceConfig) throws TException {
        // We can't use the pool yet because it does things like setting the connection's keyspace for us
        if (!attemptToCreateKeyspace(cassandraKeyspaceConfig)) {
            throw new TException("No host tried was able to create the keyspace requested.");
        }
    }

    private static boolean attemptToCreateKeyspace(CassandraKeyspaceConfig cassandraKeyspaceConfig) {
        Set<InetSocketAddress> thriftHosts =
                cassandraKeyspaceConfig.servers().accept(new ThriftHostsExtractingVisitor());

        return thriftHosts.stream().anyMatch(host -> attemptToCreateIfNotExists(host, cassandraKeyspaceConfig));
    }

    private static boolean attemptToCreateIfNotExists(
            InetSocketAddress host, CassandraKeyspaceConfig cassandraKeyspaceConfig) {
        try {
            return keyspaceAlreadyExists(host, cassandraKeyspaceConfig)
                    || attemptToCreateKeyspaceOnHost(host, cassandraKeyspaceConfig);
        } catch (Exception exception) {
            log.warn(
                    "Couldn't use host {} to create keyspace."
                            + " It returned exception \"{}\" during the attempt."
                            + " We will retry on other nodes, so this shouldn't be a problem unless all nodes failed."
                            + " See the debug-level log for the stack trace.",
                    SafeArg.of("host", CassandraLogHelper.host(host)),
                    UnsafeArg.of("exceptionMessage", exception.toString()));
            log.debug("Specifically, creating the keyspace failed with the following stack trace", exception);
            return false;
        }
    }

    // swallows the expected TException subtype NotFoundException, throws connection problem related ones
    private static boolean keyspaceAlreadyExists(
            InetSocketAddress host, CassandraKeyspaceConfig cassandraKeyspaceConfig) throws TException {
        try (CassandraClient client =
                CassandraClientFactory.getClientInternal(host, cassandraKeyspaceConfig.clientConfig())) {
            client.describe_keyspace(cassandraKeyspaceConfig.keyspace());
            CassandraKeyValueServices.waitForSchemaVersions(
                    cassandraKeyspaceConfig.schemaMutationTimeoutMillis(),
                    client,
                    "while checking if schemas diverged on startup");
            return true;
        } catch (NotFoundException e) {
            return false;
        }
    }

    private static boolean attemptToCreateKeyspaceOnHost(
            InetSocketAddress host, CassandraKeyspaceConfig cassandraKeyspaceConfig) throws TException {
        try (CassandraClient client =
                CassandraClientFactory.getClientInternal(host, cassandraKeyspaceConfig.clientConfig())) {
            KsDef ksDef = createKsDefForFresh(client, cassandraKeyspaceConfig);
            client.system_add_keyspace(ksDef);
            log.info("Created keyspace: {}", SafeArg.of("keyspace", cassandraKeyspaceConfig.keyspace()));
            CassandraKeyValueServices.waitForSchemaVersions(
                    cassandraKeyspaceConfig.schemaMutationTimeoutMillis(),
                    client,
                    "after adding the initial empty keyspace");
            return true;
        } catch (InvalidRequestException e) {
            boolean keyspaceAlreadyExists = keyspaceAlreadyExists(host, cassandraKeyspaceConfig);
            if (!keyspaceAlreadyExists) {
                log.info(
                        "Encountered an invalid request exception {} when attempting to create a keyspace"
                                + " on a given Cassandra host {}, but the keyspace doesn't seem to exist yet. This may"
                                + " cause issues if it recurs persistently, so logging for debugging purposes.",
                        SafeArg.of("host", CassandraLogHelper.host(host)),
                        UnsafeArg.of("exceptionMessage", e.toString()));
                log.debug("Specifically, creating the keyspace failed with the following stack trace", e);
            }
            return keyspaceAlreadyExists;
        }
    }

    private static void updateExistingKeyspace(
            CassandraClientPool clientPool, CassandraKeyspaceConfig cassandraKeyspaceConfig) throws TException {
        clientPool.runWithRetry((FunctionCheckedException<CassandraClient, Void, TException>) client -> {
            KsDef originalKsDef = client.describe_keyspace(cassandraKeyspaceConfig.keyspace());
            // there was an existing keyspace
            // check and make sure it's definition is up to date with our config
            KsDef modifiedKsDef = originalKsDef.deepCopy();
            checkAndSetReplicationFactor(client, modifiedKsDef, cassandraKeyspaceConfig);

            if (!modifiedKsDef.equals(originalKsDef)) {
                // Can't call system_update_keyspace to update replication factor if CfDefs are set
                modifiedKsDef.setCf_defs(ImmutableList.of());
                client.system_update_keyspace(modifiedKsDef);
                CassandraKeyValueServices.waitForSchemaVersions(
                        cassandraKeyspaceConfig.schemaMutationTimeoutMillis(),
                        client,
                        "after updating the existing keyspace");
            }
            return null;
        });
    }

    static KsDef createKsDefForFresh(CassandraClient client, CassandraKeyspaceConfig cassandraKeyspaceConfig) {
        KsDef ksDef =
                new KsDef(cassandraKeyspaceConfig.keyspace(), CassandraConstants.NETWORK_STRATEGY, ImmutableList.of());
        Set<String> dcs = sanityCheckDatacenters(
                client,
                cassandraKeyspaceConfig.servers(),
                cassandraKeyspaceConfig.replicationFactor(),
                cassandraKeyspaceConfig.ignoreNodeTopologyChecks());
        ksDef.setStrategy_options(
                Maps.asMap(dcs, ignore -> String.valueOf(cassandraKeyspaceConfig.replicationFactor())));
        ksDef.setDurable_writes(true);
        return ksDef;
    }

    static KsDef checkAndSetReplicationFactor(
            CassandraClient client, KsDef ksDef, CassandraKeyspaceConfig cassandraKeyspaceConfig) {
        KsDef result = ksDef;
        Set<String> datacenters;
        if (Objects.equals(result.getStrategy_class(), CassandraConstants.SIMPLE_STRATEGY)) {
            datacenters = getDcForSimpleStrategy(client, result, cassandraKeyspaceConfig);
            result = setNetworkStrategyIfCheckedTopology(
                    result, datacenters, cassandraKeyspaceConfig.ignoreNodeTopologyChecks());
        } else {
            datacenters = sanityCheckDatacenters(
                    client,
                    cassandraKeyspaceConfig.servers(),
                    cassandraKeyspaceConfig.replicationFactor(),
                    cassandraKeyspaceConfig.ignoreNodeTopologyChecks());
        }

        sanityCheckReplicationFactor(
                result,
                cassandraKeyspaceConfig.replicationFactor(),
                datacenters,
                cassandraKeyspaceConfig.ignoreDatacenterConfigurationChecks());
        return result;
    }

    private static Set<String> getDcForSimpleStrategy(
            CassandraClient client, KsDef ksDef, CassandraKeyspaceConfig cassandraKeyspaceConfig) {
        checkKsDefRfEqualsOne(ksDef, cassandraKeyspaceConfig.ignoreNodeTopologyChecks());
        Set<String> datacenters = sanityCheckDatacenters(
                client,
                cassandraKeyspaceConfig.servers(),
                cassandraKeyspaceConfig.replicationFactor(),
                cassandraKeyspaceConfig.ignoreNodeTopologyChecks());
        checkOneDatacenter(datacenters, cassandraKeyspaceConfig.ignoreNodeTopologyChecks());
        return datacenters;
    }

    private static KsDef setNetworkStrategyIfCheckedTopology(
            KsDef ksDef, Set<String> datacenters, boolean ignoreNodeTopologyChecks) {
        if (!ignoreNodeTopologyChecks) {
            ksDef.setStrategy_class(CassandraConstants.NETWORK_STRATEGY);
            ksDef.setStrategy_options(ImmutableMap.of(Iterables.getOnlyElement(datacenters), "1"));
        }
        return ksDef;
    }

    private static void checkKsDefRfEqualsOne(KsDef ks, boolean ignoreNodeTopologyChecks) {
        int currentRf = Integer.parseInt(ks.getStrategy_options().get(CassandraConstants.REPLICATION_FACTOR_OPTION));
        if (currentRf != 1) {
            logErrorOrThrow(SIMPLE_PARTITIONING_ERROR_MSG, ignoreNodeTopologyChecks);
        }
    }

    private static void checkOneDatacenter(Set<String> datacenters, boolean ignoreNodeTopologyChecks) {
        if (datacenters.size() > 1) {
            logErrorOrThrow(SIMPLE_PARTITIONING_ERROR_MSG, ignoreNodeTopologyChecks);
        }
    }

    static void currentRfOnKeyspaceMatchesDesiredRf(
            CassandraClient client,
            CassandraKeyspaceConfig cassandraKeyspaceConfig)
            throws TException {
        KsDef ks = client.describe_keyspace(cassandraKeyspaceConfig.keyspace());
        Set<String> dcs =
                sanityCheckDatacenters(client, cassandraKeyspaceConfig.servers(), cassandraKeyspaceConfig.replicationFactor(),
                        cassandraKeyspaceConfig.ignoreNodeTopologyChecks());
        sanityCheckReplicationFactor(
                ks, cassandraKeyspaceConfig.replicationFactor(), dcs, cassandraKeyspaceConfig.ignoreDatacenterConfigurationChecks());
    }

    static void sanityCheckReplicationFactor(
            KsDef ks, int replicationFactor, Set<String> dcs, boolean ignoreDatacentreConfigurationChecks) {
        Set<String> scopedDownDcs = checkRfsSpecifiedAndScopeDownDcs(dcs, ks.getStrategy_options());
        checkRfsMatchConfig(
                ks, replicationFactor, scopedDownDcs, ks.getStrategy_options(), ignoreDatacentreConfigurationChecks);
    }

    private static Set<String> checkRfsSpecifiedAndScopeDownDcs(Set<String> dcs, Map<String, String> strategyOptions) {
        return Sets.intersection(dcs, strategyOptions.keySet());
    }

    private static void checkRfsMatchConfig(
            KsDef ks,
            int replicationFactor,
            Set<String> dcs,
            Map<String, String> strategyOptions,
            boolean ignoreDatacenterConfigurationChecks) {
        for (String datacenter : dcs) {
            if (Integer.parseInt(strategyOptions.get(datacenter)) != replicationFactor) {
                logErrorOrThrow(
                        "Your current Cassandra keyspace (" + ks.getName()
                                + ") has a replication factor not matching your Atlas Cassandra configuration."
                                + " Change them to match, but be mindful of what steps you'll need to"
                                + " take to correctly repair or cleanup existing data in your cluster.",
                        ignoreDatacenterConfigurationChecks);
            }
        }
    }

    @Value.Immutable
    interface CassandraKeyspaceConfig {
        String keyspace();

        CassandraKeyValueServiceConfig clientConfig();

        CassandraServersConfig servers();

        int replicationFactor();

        boolean ignoreNodeTopologyChecks();

        boolean ignoreDatacenterConfigurationChecks();

        int schemaMutationTimeoutMillis();

        static CassandraKeyspaceConfig of(CassandraKeyValueServiceConfig config) {
            return builder()
                    .keyspace(config.getKeyspaceOrThrow())
                    .schemaMutationTimeoutMillis(config.schemaMutationTimeoutMillis())
                    .servers(config.servers())
                    .ignoreDatacenterConfigurationChecks(config.ignoreDatacenterConfigurationChecks())
                    .ignoreNodeTopologyChecks(config.ignoreNodeTopologyChecks())
                    .replicationFactor(config.replicationFactor())
                    .clientConfig(config)
                    .build();
        }

        static ImmutableCassandraKeyspaceConfig.Builder builder() {
            return ImmutableCassandraKeyspaceConfig.builder();
        }
    }
}
