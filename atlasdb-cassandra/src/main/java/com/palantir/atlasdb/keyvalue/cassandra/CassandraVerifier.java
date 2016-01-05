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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.EndpointDetails;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.commons.lang.Validate;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.common.collect.Maps2;


public class CassandraVerifier {
    private static final Logger log = LoggerFactory.getLogger(CassandraVerifier.class);

    // This method exists to verify a particularly nasty bug where cassandra doesn't have a
    // consistent ring across all of it's nodes.  One node will think it owns more than the others
    // think it does and they will not send writes to it, but it will respond to requests
    // acting like it does.
    protected static void sanityCheckRingConsistency(Set<String> currentHosts, int port, String keyspace, boolean isSsl, boolean safetyDisabled, int socketTimeoutMillis, int socketQueryTimeoutMillis) {
        Multimap<Set<TokenRange>, String> tokenRangesToHost = HashMultimap.create();
        for (String host : currentHosts) {
            Cassandra.Client client = null;
            try {
                client = CassandraClientFactory.getClientInternal(host, port, isSsl, socketTimeoutMillis, socketQueryTimeoutMillis);
                try {
                    client.describe_keyspace(keyspace);
                } catch (NotFoundException e) {
                    log.info("Tried to check ring consistency for node " + host + " before keyspace was fully setup; aborting check for now.", e);
                    return;
                }
                tokenRangesToHost.put(ImmutableSet.copyOf(client.describe_ring(keyspace)), host);
            } catch (Exception e) {
                log.warn("failed to get ring info from host: {}", host, e);
            } finally {
                if (client != null) {
                    client.getOutputProtocol().getTransport().close();
                }
            }
        }

        if (tokenRangesToHost.isEmpty()) {
            log.error("Failed to get ring info for entire Cassandra cluster (" + keyspace + "); ring could not be checked for consistency.");
            return;
        }

        if (tokenRangesToHost.keySet().size() == 1) {
            return;
        }

        RuntimeException e = new IllegalStateException("Hosts have differing ring descriptions.  This can lead to inconsistent reads and lost data. ");
        log.error("QA-86204 " + e.getMessage() + tokenRangesToHost, e);

        if (tokenRangesToHost.size() > 2) {
            for (Entry<Set<TokenRange>, Collection<String>> entry : tokenRangesToHost.asMap().entrySet()) {
                if (entry.getValue().size() == 1) {
                    log.error("Host: " + entry.getValue().iterator().next() +
                            " disagrees with the other nodes about the ring state.");
                }
            }
        }

        if (tokenRangesToHost.keySet().size() == 2) {
            ImmutableList<Set<TokenRange>> sets = ImmutableList.copyOf(tokenRangesToHost.keySet());
            Set<TokenRange> set1 = sets.get(0);
            Set<TokenRange> set2 = sets.get(1);
            log.error("Hosts are split.  group1: " + tokenRangesToHost.get(set1) +
                    " group2: " + tokenRangesToHost.get(set2));
        }

        logErrorOrThrow(e.getMessage(), safetyDisabled);
    }

    static Set<String> sanityCheckDatacenters(Cassandra.Client client, int desiredRf, boolean safetyDisabled) throws InvalidRequestException, TException {
        ensureTestKeyspaceExists(client);
        Multimap<String, String> dataCenterToRack = HashMultimap.create();
        List<TokenRange> ring = client.describe_ring(CassandraConstants.SIMPLE_RF_TEST_KEYSPACE);
        for (TokenRange tokenRange : ring) {
            for (EndpointDetails details : tokenRange.getEndpoint_details()) {
                dataCenterToRack.put(details.datacenter, details.rack);
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
        }

        return dataCenterToRack.keySet();
    }

    static void sanityCheckTableName(String table) {
        Validate.isTrue(!(table.startsWith("_") && table.contains("."))
                || AtlasDbConstants.hiddenTables.contains(table)
                || table.startsWith(AtlasDbConstants.NAMESPACE_PREFIX), "invalid tableName: " + table);
    }

    private static void logErrorOrThrow(String errorMessage, boolean safetyDisabled) {
        String safetyMessage = " This would have normally resulted in Palantir exiting, however safety checks have been disabled.";
        if (safetyDisabled) {
            log.error(errorMessage + safetyMessage);
        } else {
            throw new IllegalStateException(errorMessage);
        }
    }

    /*
     * This keyspace exists because we need a way to pull the datacenter information and they only
     * way to do it is if you have a valid keyspace set up.  We will pull the info from here
     * so we can accurately create the actually NetworkTopologyStrategy keyspace.
     */
    private static void ensureTestKeyspaceExists(Cassandra.Client client) {
        try {
            try {
                client.describe_keyspace(CassandraConstants.SIMPLE_RF_TEST_KEYSPACE);
                return;
            } catch (NotFoundException e) {
                // need to create key space
            }
            KsDef testKs = new KsDef(CassandraConstants.SIMPLE_RF_TEST_KEYSPACE, CassandraConstants.SIMPLE_STRATEGY, ImmutableList.<CfDef>of());
            testKs.setStrategy_options(ImmutableMap.of(CassandraConstants.REPLICATION_FACTOR_OPTION, "1"));
            client.system_add_keyspace(testKs);
        } catch (Exception e) {
            log.warn(e.getMessage(), e);
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
           String errorMessage = "This cassandra cluster is running using the simple partitioning stragegy.  " +
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
        if (currentRF == 1 && desiredRf == 2) {
            log.error("Upping AtlasDB replication factor from 1 to 2. User should run " +
                    "`nodetool repair` on cluster if they have not already!");
            strategyOptions.put(dc, String.valueOf(desiredRf));
            ks.setStrategy_options(strategyOptions);
        } else if (currentRF == 1 && desiredRf == CassandraConstants.DEFAULT_REPLICATION_FACTOR) {
            log.error("Upping AtlasDB replication factor from 1 " +
                    "to 3 is NOT allowed directly.\n" +
                    "Increase replication factor to 2 first, then run `nodetool repair`. If it succeeds, increase replication factor to 3, and run `nodetool repair`");
        } else if (currentRF == 2 && desiredRf == CassandraConstants.DEFAULT_REPLICATION_FACTOR) {
            strategyOptions.put(dc, String.valueOf(desiredRf));
            ks.setStrategy_options(strategyOptions);
            ks.setCf_defs(ImmutableList.<CfDef>of());
            client.system_update_keyspace(ks);
            log.warn("Updating AtlasDB replication factor from " + currentRF + " to " + desiredRf +
                    " process are NOT completed!" +
                    " User may want to run `nodetool repair` to make all replicas consistent.");
        } else if (currentRF > desiredRf) { // We are moving to a lower RF, this should be always safe from a consistency rule standpoint
            log.error("Reducing AtlasDB replication factor from " + currentRF + " to " + desiredRf + ". User may want to run `nodetool cleanup` to remove excess replication.");
            strategyOptions.put(dc, String.valueOf(desiredRf));
            ks.setStrategy_options(strategyOptions);
            ks.setCf_defs(ImmutableList.<CfDef>of());
            client.system_update_keyspace(ks);
        } else if (currentRF == desiredRf) {
            log.info("Did not change AtlasDB replication factor.");
        } else {
            logErrorOrThrow("We only support replication up to 3.  Attempted to go from " + currentRF + " to " + desiredRf + ".", safetyDisabled);
        }
    }
}
