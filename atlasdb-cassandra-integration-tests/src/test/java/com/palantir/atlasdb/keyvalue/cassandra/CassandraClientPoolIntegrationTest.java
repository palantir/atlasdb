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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.containers.CassandraResource;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.common.base.FunctionCheckedException;

public class CassandraClientPoolIntegrationTest {
    @ClassRule
    public static final CassandraResource CASSANDRA = new CassandraResource();

    private final MetricsManager metricsManager = MetricsManagers.createForTests();

    private int modifiedReplicationFactor;
    private Blacklist blacklist;
    private CassandraClientPoolImpl clientPool;

    @Before
    public void setUp() {
        blacklist = new Blacklist(CASSANDRA.getConfig());
        modifiedReplicationFactor = CASSANDRA.getConfig().replicationFactor() + 1;
        clientPool = CassandraClientPoolImpl.createImplForTest(metricsManager,
                CASSANDRA.getConfig(), CassandraClientPoolImpl.StartupChecks.RUN, blacklist);
    }

    @Test
    public void testTokenMapping() {
        Map<Range<LightweightOppToken>, List<InetSocketAddress>> mapOfRanges =
                clientPool.getTokenMap().asMapOfRanges();
        assertFalse(mapOfRanges.isEmpty());
        for (Entry<Range<LightweightOppToken>, List<InetSocketAddress>> entry :
                mapOfRanges.entrySet()) {
            Range<LightweightOppToken> tokenRange = entry.getKey();
            List<InetSocketAddress> hosts = entry.getValue();

            clientPool.getRandomHostForKey("A".getBytes(StandardCharsets.UTF_8));

            if (tokenRange.hasLowerBound()) {
                assertTrue(hosts.contains(clientPool.getRandomHostForKey(tokenRange.lowerEndpoint().bytes)));
            }
            if (tokenRange.hasUpperBound()) {
                assertTrue(hosts.contains(clientPool.getRandomHostForKey(tokenRange.upperEndpoint().bytes)));
            }
        }
    }

    @Test
    public void testSanitiseReplicationFactorPassesForTheKeyspace() {
        clientPool.run(client -> {
            try {
                CassandraVerifier.currentRfOnKeyspaceMatchesDesiredRf(client, CASSANDRA.getConfig());
            } catch (TException e) {
                fail("currentRf On Keyspace does not Match DesiredRf");
            }
            return false;
        });
    }

    @Test
    public void testSanitiseReplicationFactorFailsAfterManipulatingReplicationFactorInConfig() {
        clientPool.run(client -> {
            try {
                CassandraVerifier.currentRfOnKeyspaceMatchesDesiredRf(client,
                        ImmutableCassandraKeyValueServiceConfig.copyOf(
                                CASSANDRA.getConfig()).withReplicationFactor(
                                modifiedReplicationFactor));
                fail("currentRf On Keyspace Matches DesiredRf after manipulating the cassandra config");
            } catch (Exception e) {
                assertReplicationFactorMismatchError(e);
            }
            return false;
        });
    }

    @Test
    public void testSanitiseReplicationFactorFailsAfterManipulatingReplicationFactorOnCassandra() throws TException {
        changeReplicationFactor(modifiedReplicationFactor);
        clientPool.run(client -> {
            try {
                CassandraVerifier.currentRfOnKeyspaceMatchesDesiredRf(client, CASSANDRA.getConfig());
                fail("currentRf On Keyspace Matches DesiredRf after manipulating the cassandra keyspace");
            } catch (Exception e) {
                assertReplicationFactorMismatchError(e);
            }
            return false;
        });
        changeReplicationFactor(CASSANDRA.getConfig().replicationFactor());
    }

    @Test
    public void testPoolGivenNoOptionTalksToBlacklistedHosts() {
        blacklist.addAll(clientPool.getCurrentPools().keySet());
        try {
            clientPool.run(describeRing);
        } catch (Exception e) {
            fail("Should have been allowed to attempt forward progress after blacklisting all hosts in pool.");
        }

        blacklist.removeAll();
    }

    private void assertReplicationFactorMismatchError(Exception ex) {
        assertThat(ex.getMessage(), is("Your current Cassandra keyspace ("
                + CASSANDRA.getConfig().getKeyspaceOrThrow()
                + ") has a replication factor not matching your Atlas Cassandra configuration. Change them to match, "
                + "but be mindful of what steps you'll need to take to correctly repair or cleanup existing data in "
                + "your cluster."));
    }

    private void changeReplicationFactor(int replicationFactor) throws TException {
        clientPool.run((FunctionCheckedException<CassandraClient, Void, TException>) client -> {
            KsDef originalKsDef = client.describe_keyspace(CASSANDRA.getConfig().getKeyspaceOrThrow());
            KsDef modifiedKsDef = originalKsDef.deepCopy();
            modifiedKsDef.setStrategy_class(CassandraConstants.NETWORK_STRATEGY);
            modifiedKsDef.setStrategy_options(ImmutableMap.of("dc1", Integer.toString(replicationFactor)));
            modifiedKsDef.setCf_defs(ImmutableList.of());
            client.system_update_keyspace(modifiedKsDef);
            return null;
        });
    }

    private FunctionCheckedException<CassandraClient, List<TokenRange>, Exception> describeRing =
            client -> client.describe_ring(CASSANDRA.getConfig().getKeyspaceOrThrow());
}
