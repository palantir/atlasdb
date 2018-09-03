/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.cassandra;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.thrift.EndpointDetails;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Verify;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;

public final class CassandraVerifier {
    private static final Logger log = LoggerFactory.getLogger(CassandraVerifier.class);
    private static final KsDef SIMPLE_RF_TEST_KS_DEF = new KsDef(
            CassandraConstants.SIMPLE_RF_TEST_KEYSPACE,
            CassandraConstants.SIMPLE_STRATEGY,
            ImmutableList.of())
            .setStrategy_options(ImmutableMap.of(CassandraConstants.REPLICATION_FACTOR_OPTION, "1"));
    private static final String SIMPLE_PARTITIONING_ERROR_MSG = "This cassandra cluster is running using the simple "
            + "partitioning strategy. This partitioner is not rack aware and is not intended for use on prod. "
            + "This will have to be fixed by manually configuring to the network partitioner "
            + "and running the appropriate repairs; talking to support first is recommended. "
            + "If you're running in some sort of environment where nodes have no known correlated "
            + "failure patterns, you can set the 'ignoreNodeTopologyChecks' KVS config option.";

    private CassandraVerifier() {
        // Utility class
    }

    static final FunctionCheckedException<CassandraClient, Void, Exception> healthCheck = client -> {
        client.describe_version();
        return null;
    };

    static Set<String> sanityCheckDatacenters(CassandraClient client, CassandraKeyValueServiceConfig config)
            throws TException {
        createSimpleRfTestKeyspaceIfNotExists(client);

        Multimap<String, String> dataCenterToRack = HashMultimap.create();
        Set<String> hosts = Sets.newHashSet();
        for (TokenRange tokenRange : client.describe_ring(CassandraConstants.SIMPLE_RF_TEST_KEYSPACE)) {
            for (EndpointDetails details : tokenRange.getEndpoint_details()) {
                dataCenterToRack.put(details.datacenter, details.rack);
                hosts.add(details.host);
            }
        }

        if (clusterHasExactlyOneDatacenter(dataCenterToRack) && config.replicationFactor() > 1) {
            checkNodeTopologyIsSet(config, dataCenterToRack);
            checkMoreRacksThanRfOrFewerHostsThanRf(config, hosts, dataCenterToRack);
        }

        return dataCenterToRack.keySet();
    }

    private static void createSimpleRfTestKeyspaceIfNotExists(CassandraClient client) throws TException {
        List<String> existingKeyspaces = Lists.transform(client.describe_keyspaces(), KsDef::getName);
        if (!existingKeyspaces.contains(CassandraConstants.SIMPLE_RF_TEST_KEYSPACE)) {
            client.system_add_keyspace(SIMPLE_RF_TEST_KS_DEF);
        }
    }

    private static boolean clusterHasExactlyOneDatacenter(Multimap<String, String> dataCenterToRack) {
        return dataCenterToRack.keySet().size() == 1;
    }

    private static boolean clusterHasExactlyOneRack(Multimap<String, String> dataCenterToRack) {
        return dataCenterToRack.values().size() == 1;
    }

    private static void checkMoreRacksThanRfOrFewerHostsThanRf(CassandraKeyValueServiceConfig config, Set<String> hosts,
            Multimap<String, String> dcRack) {
        if (dcRack.values().size() < config.replicationFactor() && hosts.size() > config.replicationFactor()) {
            logErrorOrThrow("The cassandra cluster only has one DC, "
                            + "and is set up with less racks than the desired number of replicas, "
                            + "and there are more hosts than the replication factor. "
                            + "It is very likely that your rack configuration is incorrect and replicas "
                            + "would not be placed correctly for the failure tolerance you want. "
                            + "If you fully understand how NetworkTopology replica placement strategy will be placing "
                            + "your replicas, feel free to set the 'ignoreNodeTopologyChecks' KVS config option.",
                    config.ignoreNodeTopologyChecks());
        }
    }

    private static void checkNodeTopologyIsSet(CassandraKeyValueServiceConfig config, Multimap<String, String> dcRack) {
        if (clusterHasExactlyOneRack(dcRack)) {
            String dataCenter = Iterables.getOnlyElement(dcRack.keySet());
            String rack = Iterables.getOnlyElement(dcRack.values());
            if (dataCenter.equals(CassandraConstants.DEFAULT_DC) && rack.equals(CassandraConstants.DEFAULT_RACK)) {
                logErrorOrThrow("The cassandra cluster is not set up to be datacenter and rack aware. Please set up "
                                + "Cassandra to use NetworkTopology and add corresponding snitch information "
                                + "before running with a replication factor higher than 1. "
                                + "If you're running in some sort of environment where nodes have no known correlated "
                                + "failure patterns, you can set the 'ignoreNodeTopologyChecks' KVS config option.",
                        config.ignoreNodeTopologyChecks());

            }
        }
    }

    @SuppressWarnings("ValidateConstantMessage") // https://github.com/palantir/gradle-baseline/pull/175
    static void sanityCheckTableName(TableReference tableRef) {
        String tableName = tableRef.getQualifiedName();
        Validate.isTrue(!(tableName.startsWith("_") && tableName.contains("."))
                || AtlasDbConstants.hiddenTables.contains(tableRef)
                || tableName.startsWith(AtlasDbConstants.NAMESPACE_PREFIX),
                "invalid tableName: %s", tableName);
    }

    /**
     * If {@code shouldLogAndNotThrow} is set, log the {@code errorMessage} at error.
     * The {@code errorMessage} is considered safe to be logged.
     * If {@code shouldLogAndNotThrow} is not set, an IllegalStateException is thrown with message {@code errorMessage}.
     */
    static void logErrorOrThrow(String errorMessage, boolean shouldLogAndNotThrow) {
        if (shouldLogAndNotThrow) {
            log.error("{} This would have normally resulted in Palantir exiting however "
                    + "safety checks have been disabled.", SafeArg.of("errorMessage", errorMessage));
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

    static void ensureKeyspaceExistsAndIsUpToDate(CassandraClientPool clientPool, CassandraKeyValueServiceConfig config)
            throws TException {
        createKeyspace(config);
        updateExistingKeyspace(clientPool, config);
    }

    private static void createKeyspace(CassandraKeyValueServiceConfig config) throws TException {
        // We can't use the pool yet because it does things like setting the connection's keyspace for us
        if (!attemptToCreateKeyspace(config)) {
            throw new TException("No host tried was able to create the keyspace requested.");
        }
    }

    // Returns true iff we successfully created the keyspace on some host.
    private static boolean attemptToCreateKeyspace(CassandraKeyValueServiceConfig config)
            throws InvalidRequestException {
        for (InetSocketAddress host : config.servers()) { // try until we find a server that works
            try {
                if (!keyspaceAlreadyExists(host, config)) {
                    attemptToCreateKeyspaceOnHost(host, config);
                }

                // if we got this far, we're done, no need to continue on other hosts
                return true;
            } catch (InvalidRequestException ire) {
                if (attemptedToCreateKeyspaceTwice(ire)) {
                    // if we got this far, we're done, no need to continue on other hosts
                    return true;
                } else {
                    throw ire;
                }
            } catch (Exception exception) {
                log.warn("Couldn't use host {} to create keyspace."
                        + " It returned exception \"{}\" during the attempt."
                        + " We will retry on other nodes, so this shouldn't be a problem unless all nodes failed."
                        + " See the debug-level log for the stack trace.",
                        SafeArg.of("host", CassandraLogHelper.host(host)),
                        UnsafeArg.of("exceptionMessage", exception.toString()));
                log.debug("Specifically, creating the keyspace failed with the following stack trace", exception);
            }
        }
        return false;
    }

    // swallows the expected TException subtype NotFoundException, throws connection problem related ones
    private static boolean keyspaceAlreadyExists(InetSocketAddress host, CassandraKeyValueServiceConfig config)
            throws TException {
        try {
            CassandraClient client = CassandraClientFactory.getClientInternal(host, config);
            client.describe_keyspace(config.getKeyspaceOrThrow());
            CassandraKeyValueServices.waitForSchemaVersions(
                    config,
                    client,
                    "(checking if schemas diverged on startup)",
                    true);
            return true;
        } catch (NotFoundException e) {
            return false;
        }
    }

    private static void attemptToCreateKeyspaceOnHost(InetSocketAddress host, CassandraKeyValueServiceConfig config)
            throws TException {
        CassandraClient client = CassandraClientFactory.getClientInternal(host, config);
        KsDef ksDef = createKsDefForFresh(client, config);
        client.system_add_keyspace(ksDef);
        log.info("Created keyspace: {}", UnsafeArg.of("keyspace", config.getKeyspaceOrThrow()));
        CassandraKeyValueServices.waitForSchemaVersions(
                config,
                client,
                "(adding the initial empty keyspace)",
                true);
    }

    private static boolean attemptedToCreateKeyspaceTwice(InvalidRequestException ex) {
        String exceptionString = ex.toString();
        if (exceptionString.contains("case-insensitively unique")) {
            String[] keyspaceNames = StringUtils.substringsBetween(exceptionString, "\"", "\"");
            if (keyspaceNames.length == 2 && keyspaceNames[0].equals(keyspaceNames[1])) {
                return true;
            }
        }
        return false;
    }


    private static void updateExistingKeyspace(CassandraClientPool clientPool, CassandraKeyValueServiceConfig config)
            throws TException {
        clientPool.runWithRetry((FunctionCheckedException<CassandraClient, Void, TException>) client -> {
            KsDef originalKsDef = client.describe_keyspace(config.getKeyspaceOrThrow());
            // there was an existing keyspace
            // check and make sure it's definition is up to date with our config
            KsDef modifiedKsDef = originalKsDef.deepCopy();
            checkAndSetReplicationFactor(
                    client,
                    modifiedKsDef,
                    config);

            if (!modifiedKsDef.equals(originalKsDef)) {
                // Can't call system_update_keyspace to update replication factor if CfDefs are set
                modifiedKsDef.setCf_defs(ImmutableList.of());
                client.system_update_keyspace(modifiedKsDef);
                CassandraKeyValueServices.waitForSchemaVersions(
                        config,
                        client,
                        "(updating the existing keyspace)");
            }

            return null;
        });
    }

    static KsDef createKsDefForFresh(CassandraClient client, CassandraKeyValueServiceConfig config) throws TException {
        KsDef ksDef = new KsDef(config.getKeyspaceOrThrow(), CassandraConstants.NETWORK_STRATEGY,
                ImmutableList.of());
        Set<String> dcs = sanityCheckDatacenters(client, config);
        ksDef.setStrategy_options(Maps.asMap(dcs, ignore -> String.valueOf(config.replicationFactor())));
        ksDef.setDurable_writes(true);
        return ksDef;
    }

    static KsDef checkAndSetReplicationFactor(CassandraClient client, KsDef ksDef,
            CassandraKeyValueServiceConfig config) throws TException {

        Set<String> dataCenters;
        if (CassandraConstants.SIMPLE_STRATEGY.equals(ksDef.getStrategy_class())) {
            dataCenters = getDcForSimpleStrategy(client, ksDef, config);
            ksDef = setNetworkStrategyIfCheckedTopology(ksDef, config, dataCenters);
        } else {
            dataCenters = sanityCheckDatacenters(client, config);
        }

        sanityCheckReplicationFactor(ksDef, config, dataCenters);
        return ksDef;
    }

    private static Set<String> getDcForSimpleStrategy(CassandraClient client, KsDef ksDef,
            CassandraKeyValueServiceConfig config) throws TException {
        checkKsDefRfEqualsOne(ksDef, config);
        Set<String> dataCenters = sanityCheckDatacenters(client, config);
        checkOneDatacenter(config, dataCenters);
        return dataCenters;
    }

    private static KsDef setNetworkStrategyIfCheckedTopology(KsDef ksDef, CassandraKeyValueServiceConfig config,
            Set<String> dataCenters) {
        if (!config.ignoreNodeTopologyChecks()) {
            ksDef.setStrategy_class(CassandraConstants.NETWORK_STRATEGY);
            ksDef.setStrategy_options(ImmutableMap.of(Iterables.getOnlyElement(dataCenters), "1"));
        }
        return ksDef;
    }

    private static void checkKsDefRfEqualsOne(KsDef ks, CassandraKeyValueServiceConfig config) {
        int currentRf = Integer.parseInt(ks.getStrategy_options().get(CassandraConstants.REPLICATION_FACTOR_OPTION));
        if (currentRf != 1) {
            logErrorOrThrow(SIMPLE_PARTITIONING_ERROR_MSG, config.ignoreNodeTopologyChecks());
        }
    }

    private static void checkOneDatacenter(CassandraKeyValueServiceConfig config, Set<String> dataCenters) {
        if (dataCenters.size() > 1) {
            logErrorOrThrow(SIMPLE_PARTITIONING_ERROR_MSG, config.ignoreNodeTopologyChecks());
        }
    }

    static void currentRfOnKeyspaceMatchesDesiredRf(CassandraClient client, CassandraKeyValueServiceConfig config)
            throws TException {
        KsDef ks = client.describe_keyspace(config.getKeyspaceOrThrow());
        Set<String> dcs = sanityCheckDatacenters(client, config);
        sanityCheckReplicationFactor(ks, config, dcs);
    }

    static void sanityCheckReplicationFactor(KsDef ks, CassandraKeyValueServiceConfig config, Set<String> dcs) {
        checkRfsSpecified(config, dcs, ks.getStrategy_options());
        checkRfsMatchConfig(ks, config, dcs, ks.getStrategy_options());
    }

    private static void checkRfsSpecified(CassandraKeyValueServiceConfig config, Set<String> dcs,
            Map<String, String> strategyOptions) {
        for (String dataCenter : dcs) {
            if (strategyOptions.get(dataCenter) == null) {
                logErrorOrThrow("The datacenter for this cassandra cluster is invalid. "
                        + " failed dc: " + dataCenter + "  strategyOptions: " + strategyOptions,
                        config.ignoreDatacenterConfigurationChecks());
            }
        }
    }

    private static void checkRfsMatchConfig(KsDef ks, CassandraKeyValueServiceConfig config, Set<String> dcs,
            Map<String, String> strategyOptions) {
        for (String dataCenter : dcs) {
            if (Integer.parseInt(strategyOptions.get(dataCenter)) != config.replicationFactor()) {
                throw new UnsupportedOperationException("Your current Cassandra keyspace (" + ks.getName()
                        + ") has a replication factor not matching your Atlas Cassandra configuration."
                        + " Change them to match, but be mindful of what steps you'll need to"
                        + " take to correctly repair or cleanup existing data in your cluster.");
            }
        }
    }

    static final FunctionCheckedException<CassandraClient, Boolean, UnsupportedOperationException>
            underlyingCassandraClusterSupportsCASOperations = client -> {
                try {
                    CassandraApiVersion serverVersion = new CassandraApiVersion(client.describe_version());
                    log.debug("Connected cassandra thrift version is: {}",
                            SafeArg.of("cassandraVersion", serverVersion));
                    return serverVersion.supportsCheckAndSet();
                } catch (TException ex) {
                    throw new UnsupportedOperationException("Couldn't determine underlying cassandra version;"
                            + " received an exception while checking the thrift version.", ex);
                }
            };
}
