/**
 * Copyright 2015 Palantir Technologies
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.EndpointDetails;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.commons.lang.Validate;
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
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientFactory.ClientCreationFailedException;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.collect.Maps2;


public class CassandraVerifier {
    private static final Logger log = LoggerFactory.getLogger(CassandraVerifier.class);

    static final FunctionCheckedException<Cassandra.Client, Void, Exception> healthCheck = new FunctionCheckedException<Cassandra.Client, Void, Exception>() {
        @Override
        public Void apply(Cassandra.Client client) throws Exception {
            client.describe_version();
            return null;
        }
    };

    static Set<String> sanityCheckDatacenters(Cassandra.Client client, int desiredRf, boolean safetyDisabled) throws InvalidRequestException, TException {
        Set<String> hosts = Sets.newHashSet();
        Multimap<String, String> dataCenterToRack = HashMultimap.create();

        List<String> existingKeyspaces = Lists.transform(client.describe_keyspaces(), KsDef::getName);
        if (!existingKeyspaces.contains(CassandraConstants.SIMPLE_RF_TEST_KEYSPACE)) {
            client.system_add_keyspace(
                    new KsDef(CassandraConstants.SIMPLE_RF_TEST_KEYSPACE, CassandraConstants.SIMPLE_STRATEGY, ImmutableList.<CfDef>of())
                    .setStrategy_options(ImmutableMap.of(CassandraConstants.REPLICATION_FACTOR_OPTION, "1")));
        }
        List<TokenRange> ring =  client.describe_ring(CassandraConstants.SIMPLE_RF_TEST_KEYSPACE);

        for (TokenRange tokenRange: ring) {
            for (EndpointDetails details : tokenRange.getEndpoint_details()) {
                dataCenterToRack.put(details.datacenter, details.rack);
                hosts.add(details.host);
            }
        }

        if (dataCenterToRack.size() == 1) {
            String dc = dataCenterToRack.keySet().iterator().next();
            String rack = dataCenterToRack.values().iterator().next();
            if (dc.equals(CassandraConstants.DEFAULT_DC) && rack.equals(CassandraConstants.DEFAULT_RACK) && desiredRf > 1) {
                // We don't allow greater than RF=1 because they didn't set up their network.
                logErrorOrThrow("The cassandra cluster is not set up to be datacenter and rack aware.  " +
                        "Please set this up before running with a replication factor higher than 1.", safetyDisabled);

            }
            if (dataCenterToRack.values().size() < desiredRf && hosts.size() > desiredRf) {
                logErrorOrThrow("The cassandra cluster only has one DC, " +
                        "and is set up with less racks than the desired number of replicas, " +
                        "and there are more hosts than the replication factor. " +
                        "It is very likely that your rack configuration is incorrect and replicas would not be placed correctly for the failure tolerance you want.", safetyDisabled);
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

    static void logErrorOrThrow(String errorMessage, boolean safetyDisabled) {
        String safetyMessage = " This would have normally resulted in Palantir exiting, however safety checks have been disabled.";
        if (safetyDisabled) {
            log.error(errorMessage + safetyMessage);
        } else {
            throw new IllegalStateException(errorMessage);
        }
    }

    static void validatePartitioner(Cassandra.Client client, CassandraKeyValueServiceConfig config) throws TException {
        String partitioner = client.describe_partitioner();
        if (!config.safetyDisabled()) {
            Verify.verify(
                    CassandraConstants.ALLOWED_PARTITIONERS.contains(partitioner),
                    "Invalid partitioner. Allowed: %s, but partitioner is: %s",
                    CassandraConstants.ALLOWED_PARTITIONERS,
                    partitioner);
        }
    }

    static void ensureKeyspaceExistsAndIsUpToDate(CassandraClientPool clientPool, CassandraKeyValueServiceConfig config) throws InvalidRequestException, TException, SchemaDisagreementException {
        try {
            clientPool.run(new FunctionCheckedException<Cassandra.Client, Void, TException>() {
                @Override
                public Void apply(Cassandra.Client client) throws TException {
                    KsDef originalKsDef = client.describe_keyspace(config.keyspace());
                    KsDef modifiedKsDef = originalKsDef.deepCopy();
                    CassandraVerifier.checkAndSetReplicationFactor(client, modifiedKsDef, false, config.replicationFactor(), config.safetyDisabled());

                    if (!modifiedKsDef.equals(originalKsDef)) {
                        modifiedKsDef.setCf_defs(ImmutableList.<CfDef>of()); // Can't call system_update_keyspace to update replication factor if CfDefs are set
                        client.system_update_keyspace(modifiedKsDef);
                        CassandraKeyValueServices.waitForSchemaVersions(client, "(updating the existing keyspace)", config.schemaMutationTimeoutMillis());
                    }

                    return null;
                }
            });
        } catch (ClientCreationFailedException e) { // We can't use the pool yet because it does things like setting the keyspace of that connection for us
            boolean someHostWasAbleToCreateTheKeyspace = false;
            for (InetSocketAddress host : config.servers()) { // try until we find a server that works
                try {
                    Client client = CassandraClientFactory.getClientInternal(host, config.ssl(), config.socketTimeoutMillis(), config.socketQueryTimeoutMillis());
                    KsDef ks = new KsDef(config.keyspace(), CassandraConstants.NETWORK_STRATEGY, ImmutableList.<CfDef>of());
                    CassandraVerifier.checkAndSetReplicationFactor(client, ks, true, config.replicationFactor(), config.safetyDisabled());
                    ks.setDurable_writes(true);
                    client.system_add_keyspace(ks);
                    CassandraKeyValueServices.waitForSchemaVersions(client, "(adding the initial empty keyspace)", config.schemaMutationTimeoutMillis());

                    // if we got this far, we're done, no need to continue on other hosts
                    someHostWasAbleToCreateTheKeyspace = true;
                    break;
                } catch (Exception f) {
                    log.error("Couldn't use host {} to create keyspace, it returned exception \"{}\" during the attempt.", host, f.toString(), f);
                }
            }
            if (someHostWasAbleToCreateTheKeyspace) {
                return;
            } else {
                throw new TException("No host tried was able to create the keyspace requested.");
            }
        }
    }

    static void checkAndSetReplicationFactor(Cassandra.Client client, KsDef ks, boolean freshInstance, int desiredRf, boolean safetyDisabled)
            throws InvalidRequestException, SchemaDisagreementException, TException {
        if (freshInstance) {
            Set<String> dcs = CassandraVerifier.sanityCheckDatacenters(client, desiredRf, safetyDisabled);
            // If RF exceeds # hosts, then Cassandra will reject writes
            ks.setStrategy_options(Maps2.createConstantValueMap(dcs, String.valueOf(desiredRf)));
            return;
        }

        final Set<String> dcs;
        if (CassandraConstants.SIMPLE_STRATEGY.equals(ks.getStrategy_class())) {
            int currentRF = Integer.parseInt(ks.getStrategy_options().get(CassandraConstants.REPLICATION_FACTOR_OPTION));
            String errorMessage = "This cassandra cluster is running using the simple partitioning strategy.  " +
                    "This partitioner is not rack aware and is not intended for use on prod.  " +
                    "This will have to be fixed by manually configuring to the network partitioner " +
                    "and running the appropriate repairs.  " +
                    "Contact the AtlasDB team to perform these steps.";
            if (currentRF != 1) {
                logErrorOrThrow(errorMessage, safetyDisabled);
            }
            // Automatically convert RF=1 to look like network partitioner.
            dcs = CassandraVerifier.sanityCheckDatacenters(client, desiredRf, safetyDisabled);
            if (dcs.size() > 1) {
                logErrorOrThrow(errorMessage, safetyDisabled);
            }
            if (!safetyDisabled) {
                ks.setStrategy_class(CassandraConstants.NETWORK_STRATEGY);
                ks.setStrategy_options(ImmutableMap.of(dcs.iterator().next(), "1"));
            }
        } else {
            dcs = CassandraVerifier.sanityCheckDatacenters(client, desiredRf, safetyDisabled);
        }

        Map<String, String> strategyOptions = Maps.newHashMap(ks.getStrategy_options());
        for (String dc : dcs) {
            if (strategyOptions.get(dc) == null) {
                logErrorOrThrow("The datacenter for this cassandra cluster is invalid. " +
                        " failed dc: " + dc +
                        "  strategyOptions: " + strategyOptions, safetyDisabled);
            }
        }

        String dc = dcs.iterator().next();

        int currentRF = Integer.parseInt(strategyOptions.get(dc));

        // We need to worry about user not running repair and user skipping replication levels.
        if (currentRF != desiredRf) {
            throw new UnsupportedOperationException("Your current Cassandra keyspace (" + ks.getName() +
                    ") has a replication factor not matching your Atlas Cassandra configuration. " +
                    "Change them to match, but be mindful of what steps you'll need to " +
                    "take to correctly repair or cleanup existing data in your cluster.");
        }
    }

    final static FunctionCheckedException<Cassandra.Client, Boolean, UnsupportedOperationException> underlyingCassandraClusterSupportsCASOperations = new FunctionCheckedException<Client, Boolean, UnsupportedOperationException>() {
        @Override
        public Boolean apply(Client client) throws UnsupportedOperationException {
            try {
                String versionString = client.describe_version();
                String[] components = versionString.split("\\.");
                if (components.length != 3) {
                    throw new UnsupportedOperationException(String.format("Illegal version of Thrift protocol detected; expected format '#.#.#', got '%s'", Arrays.toString(components)));
                }
                int majorVersion = Integer.parseInt(components[0]);
                int minorVersion = Integer.parseInt(components[1]);
                int patchVersion = Integer.parseInt(components[2]);

                // see cassandra's 'interface/cassandra.thrift'
                if (majorVersion >= 19 && minorVersion >= 37 && patchVersion >= 0) {
                    return true;
                } else {
                    return false;
                }
            } catch (TException e) {
                throw new UnsupportedOperationException("Couldn't determine underlying cassandra version; received an exception while checking the thrift version.", e);
            }
        }
    };

}
