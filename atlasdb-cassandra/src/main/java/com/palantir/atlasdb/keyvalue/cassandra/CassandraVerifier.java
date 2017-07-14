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

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Cassandra.Client;
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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.collect.Maps2;

public final class CassandraVerifier {
    private static final Logger log = LoggerFactory.getLogger(CassandraVerifier.class);

    private CassandraVerifier() {
        // Utility class
    }

    static final FunctionCheckedException<Cassandra.Client, Void, Exception> healthCheck = client -> {
        client.describe_version();
        return null;
    };

    static Set<String> sanityCheckDatacenters(
            Cassandra.Client client, CassandraKeyValueServiceConfig config)
            throws TException {
        Set<String> hosts = Sets.newHashSet();
        Multimap<String, String> dataCenterToRack = HashMultimap.create();

        List<String> existingKeyspaces = Lists.transform(client.describe_keyspaces(), KsDef::getName);
        if (!existingKeyspaces.contains(CassandraConstants.SIMPLE_RF_TEST_KEYSPACE)) {
            client.system_add_keyspace(
                    new KsDef(
                            CassandraConstants.SIMPLE_RF_TEST_KEYSPACE,
                            CassandraConstants.SIMPLE_STRATEGY,
                            ImmutableList.of())
                            .setStrategy_options(ImmutableMap.of(CassandraConstants.REPLICATION_FACTOR_OPTION, "1")));
        }
        List<TokenRange> ring =  client.describe_ring(CassandraConstants.SIMPLE_RF_TEST_KEYSPACE);

        for (TokenRange tokenRange : ring) {
            for (EndpointDetails details : tokenRange.getEndpoint_details()) {
                dataCenterToRack.put(details.datacenter, details.rack);
                hosts.add(details.host);
            }
        }

        if (dataCenterToRack.size() == 1) {
            String dc = dataCenterToRack.keySet().iterator().next();
            String rack = dataCenterToRack.values().iterator().next();
            if (dc.equals(CassandraConstants.DEFAULT_DC) && rack.equals(CassandraConstants.DEFAULT_RACK)
                    && config.replicationFactor() > 1) {
                // We don't allow greater than RF=1 because they didn't set up their node topology
                logErrorOrThrow("The cassandra cluster is not set up to be datacenter and rack aware.  "
                        + "Please set this up before running with a replication factor higher than 1.",
                        config.safetyDisabled() || config.ignoreBadNodeTopologyChecks());

            }
            if (dataCenterToRack.values().size() < config.replicationFactor()
                    && hosts.size() > config.replicationFactor()) {
                logErrorOrThrow("The cassandra cluster only has one DC, "
                        + "and is set up with less racks than the desired number of replicas, "
                        + "and there are more hosts than the replication factor. "
                        + "It is very likely that your rack configuration is incorrect and replicas "
                        + "would not be placed correctly for the failure tolerance you want.",
                        config.safetyDisabled() || config.ignoreBadNodeTopologyChecks());
            }
        }

        return dataCenterToRack.keySet();
    }

    static void sanityCheckTableName(TableReference tableRef) {
        String tableName = tableRef.getQualifiedName();
        Validate.isTrue(!(tableName.startsWith("_") && tableName.contains("."))
                || AtlasDbConstants.hiddenTables.contains(tableRef)
                || tableName.startsWith(AtlasDbConstants.NAMESPACE_PREFIX), "invalid tableName: " + tableName);
    }

    static void logErrorOrThrow(String errorMessage, boolean shouldLogAndNotThrow) {
        if (shouldLogAndNotThrow) {
            log.error("{} This would have normally resulted in Palantir exiting however "
                    + "safety checks have been disabled.", errorMessage);
        } else {
            throw new IllegalStateException(errorMessage);
        }
    }

    static void validatePartitioner(String partitioner, CassandraKeyValueServiceConfig config) {
        if (!(config.safetyDisabled() || config.ignorePartitionerChecks())) {
            Verify.verify(
                    CassandraConstants.ALLOWED_PARTITIONERS.contains(partitioner),
                    "Invalid partitioner. Allowed: %s, but partitioner is: %s",
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
            } catch (Exception f) {
                log.warn("Couldn't use host {} to create keyspace."
                        + " It returned exception \"{}\" during the attempt."
                        + " We will retry on other nodes, so this shouldn't be a problem unless all nodes failed."
                        + " See the debug-level log for the stack trace.", host, f.toString(), f);
                log.debug("Specifically, creating the keyspace failed with the following stack trace", f);
            }
        }
        return false;
    }

    // swallows the expected TException subtype NotFoundException, throws connection problem related ones
    private static boolean keyspaceAlreadyExists(InetSocketAddress host, CassandraKeyValueServiceConfig config)
            throws TException {
        try {
            Client client = CassandraClientFactory.getClientInternal(host, config);
            client.describe_keyspace(config.keyspace());
            return true;
        } catch (NotFoundException e) {
            return false;
        }
    }

    private static void attemptToCreateKeyspaceOnHost(InetSocketAddress host, CassandraKeyValueServiceConfig config)
            throws TException {
        Client client = CassandraClientFactory.getClientInternal(host, config);
        KsDef ks = new KsDef(config.keyspace(), CassandraConstants.NETWORK_STRATEGY,
                ImmutableList.of());
        checkAndSetReplicationFactor(
                client,
                ks,
                true,
                config);
        ks.setDurable_writes(true);
        client.system_add_keyspace(ks);
        log.info("Created keyspace: {}", config.keyspace());
        CassandraKeyValueServices.waitForSchemaVersions(
                client,
                "(adding the initial empty keyspace)",
                config.schemaMutationTimeoutMillis());
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
        clientPool.runWithRetry(new FunctionCheckedException<Cassandra.Client, Void, TException>() {
            @Override
            public Void apply(Cassandra.Client client) throws TException {
                KsDef originalKsDef = client.describe_keyspace(config.keyspace());
                // there was an existing keyspace
                // check and make sure it's definition is up to date with our config
                KsDef modifiedKsDef = originalKsDef.deepCopy();
                checkAndSetReplicationFactor(
                        client,
                        modifiedKsDef,
                        false,
                        config);

                if (!modifiedKsDef.equals(originalKsDef)) {
                    // Can't call system_update_keyspace to update replication factor if CfDefs are set
                    modifiedKsDef.setCf_defs(ImmutableList.of());
                    client.system_update_keyspace(modifiedKsDef);
                    CassandraKeyValueServices.waitForSchemaVersions(
                            client,
                            "(updating the existing keyspace)",
                            config.schemaMutationTimeoutMillis());
                }

                return null;
            }
        });
    }

    private static void checkAndSetReplicationFactor(
            Cassandra.Client client,
            KsDef ks,
            boolean freshInstance,
            CassandraKeyValueServiceConfig config) throws TException {
        if (freshInstance) {
            Set<String> dcs = sanityCheckDatacenters(client, config);
            // If RF exceeds # hosts, then Cassandra will reject writes
            ks.setStrategy_options(Maps2.createConstantValueMap(dcs, String.valueOf(config.replicationFactor())));
            return;
        }

        final Set<String> dcs;
        if (CassandraConstants.SIMPLE_STRATEGY.equals(ks.getStrategy_class())) {
            int currentRf = Integer.parseInt(ks.getStrategy_options().get(
                    CassandraConstants.REPLICATION_FACTOR_OPTION));
            String errorMessage = "This cassandra cluster is running using the simple partitioning strategy."
                    + " This partitioner is not rack aware and is not intended for use on prod."
                    + " This will have to be fixed by manually configuring to the network partitioner"
                    + " and running the appropriate repairs."
                    + " Contact the AtlasDB team to perform these steps.";
            if (currentRf != 1) {
                logErrorOrThrow(errorMessage, config.safetyDisabled() || config.ignoreBadNodeTopologyChecks());
            }
            // Automatically convert RF=1 to look like network partitioner.
            dcs = sanityCheckDatacenters(client, config);
            if (dcs.size() > 1) {
                logErrorOrThrow(errorMessage,
                        config.safetyDisabled() || config.ignoreBadDatacenterConfigurationChecks());
            }
            if (!(config.safetyDisabled() || config.ignoreBadDatacenterConfigurationChecks())) {
                ks.setStrategy_class(CassandraConstants.NETWORK_STRATEGY);
                ks.setStrategy_options(ImmutableMap.of(dcs.iterator().next(), "1"));
            }
        } else {
            dcs = sanityCheckDatacenters(client, config);
        }

        sanityCheckReplicationFactor(ks, config, dcs);
    }

    static void currentRfOnKeyspaceMatchesDesiredRf(Client client, CassandraKeyValueServiceConfig config)
            throws TException {
        KsDef ks = client.describe_keyspace(config.keyspace());
        Set<String> dcs = sanityCheckDatacenters(client, config);
        sanityCheckReplicationFactor(ks, config, dcs);
    }

    private static void sanityCheckReplicationFactor(KsDef ks, CassandraKeyValueServiceConfig config, Set<String> dcs) {
        Map<String, String> strategyOptions = Maps.newHashMap(ks.getStrategy_options());
        for (String dc : dcs) {
            if (strategyOptions.get(dc) == null) {
                logErrorOrThrow("The datacenter for this cassandra cluster is invalid. "
                        + " failed dc: " + dc
                        + "  strategyOptions: " + strategyOptions, config.ignoreBadDatacenterConfigurationChecks());
            }
        }

        String dc = dcs.iterator().next();

        int currentRf = Integer.parseInt(strategyOptions.get(dc));

        // We need to worry about user not running repair and user skipping replication levels.
        if (currentRf != config.replicationFactor()) {
            throw new UnsupportedOperationException("Your current Cassandra keyspace (" + ks.getName()
                    + ") has a replication factor not matching your Atlas Cassandra configuration."
                    + " Change them to match, but be mindful of what steps you'll need to"
                    + " take to correctly repair or cleanup existing data in your cluster.");
        }
    }

    static final FunctionCheckedException<Cassandra.Client, Boolean, UnsupportedOperationException>
            underlyingCassandraClusterSupportsCASOperations = client -> {
                try {
                    CassandraApiVersion serverVersion = new CassandraApiVersion(client.describe_version());
                    log.debug("Connected cassandra thrift version is: {}", serverVersion);
                    return serverVersion.supportsCheckAndSet();
                } catch (TException ex) {
                    throw new UnsupportedOperationException("Couldn't determine underlying cassandra version;"
                            + " received an exception while checking the thrift version.", ex);
                }
            };
}
