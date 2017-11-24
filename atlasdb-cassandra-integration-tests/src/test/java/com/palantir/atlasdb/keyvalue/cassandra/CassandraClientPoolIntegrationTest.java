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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.thrift.TException;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.containers.CassandraContainer;
import com.palantir.atlasdb.containers.Containers;
import com.palantir.common.base.FunctionCheckedException;

public class CassandraClientPoolIntegrationTest {
    @ClassRule
    public static final Containers CONTAINERS = new Containers(CassandraClientPoolIntegrationTest.class)
            .with(new CassandraContainer());
    private static final int MODIFIED_REPLICATION_FACTOR = CassandraContainer.KVS_CONFIG.replicationFactor() + 1;

    private Blacklist blacklist = new Blacklist(CassandraContainer.KVS_CONFIG);
    private CassandraClientPoolImpl clientPool = CassandraClientPoolImpl.createImplForTest(
            CassandraContainer.KVS_CONFIG, CassandraClientPoolImpl.StartupChecks.RUN, blacklist);

    private CassandraKeyValueService kv = CassandraKeyValueServiceImpl.create(
            CassandraKeyValueServiceConfigManager.createSimpleManager(CassandraContainer.KVS_CONFIG),
            CassandraContainer.LEADER_CONFIG,
            clientPool);


    @Before
    public void setUp() {
        kv.dropTable(AtlasDbConstants.TIMESTAMP_TABLE);
    }

    @After
    public void close() {
        kv.close();
    }

    // This is a dumb test in the current test suite that has just one local Cassandra node.
    // Pretty legit test if run manually or if we go back to multi-node tests
    @Test
    public void testTokenMapping() {
        Map<Range<LightweightOppToken>, List<InetSocketAddress>> mapOfRanges =
                clientPool.getCassandra().getTokenMap().asMapOfRanges();

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
                CassandraVerifier.currentRfOnKeyspaceMatchesDesiredRf(client.rawClient(),
                        CassandraContainer.KVS_CONFIG);
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
                CassandraVerifier.currentRfOnKeyspaceMatchesDesiredRf(client.rawClient(),
                        ImmutableCassandraKeyValueServiceConfig.copyOf(
                                CassandraContainer.KVS_CONFIG).withReplicationFactor(
                                MODIFIED_REPLICATION_FACTOR));
                fail("currentRf On Keyspace Matches DesiredRf after manipulating the cassandra config");
            } catch (Exception e) {
                assertReplicationFactorMismatchError(e);
            }
            return false;
        });
    }

    @Test
    public void testSanitiseReplicationFactorFailsAfterManipulatingReplicationFactorOnCassandra() throws TException {
        changeReplicationFactor(MODIFIED_REPLICATION_FACTOR);
        clientPool.run(client -> {
            try {
                CassandraVerifier.currentRfOnKeyspaceMatchesDesiredRf(client.rawClient(),
                        CassandraContainer.KVS_CONFIG);
                fail("currentRf On Keyspace Matches DesiredRf after manipulating the cassandra keyspace");
            } catch (Exception e) {
                assertReplicationFactorMismatchError(e);
            }
            return false;
        });
        changeReplicationFactor(CassandraContainer.KVS_CONFIG.replicationFactor());
    }

    private void assertReplicationFactorMismatchError(Exception ex) {
        assertThat(ex.getMessage(), is("Your current Cassandra keyspace (atlasdb) has a replication"
                + " factor not matching your Atlas Cassandra configuration. Change them to match, but be mindful"
                + " of what steps you'll need to take to correctly repair or cleanup existing data in your cluster."));
    }

    private void changeReplicationFactor(int replicationFactor) throws TException {
        clientPool.run((FunctionCheckedException<CassandraClient, Void, TException>) client -> {
            KsDef originalKsDef = client.rawClient().describe_keyspace(
                    CassandraContainer.KVS_CONFIG.getKeyspaceOrThrow());
            KsDef modifiedKsDef = originalKsDef.deepCopy();
            modifiedKsDef.setStrategy_class(CassandraConstants.NETWORK_STRATEGY);
            modifiedKsDef.setStrategy_options(ImmutableMap.of("dc1", Integer.toString(replicationFactor)));
            modifiedKsDef.setCf_defs(ImmutableList.of());
            client.rawClient().system_update_keyspace(modifiedKsDef);
            return null;
        });
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

    private FunctionCheckedException<CassandraClient, List<TokenRange>, Exception> describeRing =
            client -> client.rawClient().describe_ring("atlasdb");

    @Test
    public void testWeightedHostsWithUniformActivity() {
        Map<InetSocketAddress, CassandraClientPoolingContainer> pools = ImmutableMap.of(
                new InetSocketAddress(0), createMockClientPoolingContainerWithUtilization(10),
                new InetSocketAddress(1), createMockClientPoolingContainerWithUtilization(10),
                new InetSocketAddress(2), createMockClientPoolingContainerWithUtilization(10));

        NavigableMap<Integer, InetSocketAddress> result = CassandraClientPoolImpl.WeightedHosts.create(pools).hosts;

        int expectedWeight = result.firstEntry().getKey();
        int prevKey = 0;
        for (Map.Entry<Integer, InetSocketAddress> entry : result.entrySet()) {
            int currWeight = entry.getKey() - prevKey;
            assertEquals(expectedWeight, currWeight);
            prevKey = entry.getKey();
        }
    }

    @Test
    public void testWeightedHostsWithLowActivityPool() {
        InetSocketAddress lowActivityHost = new InetSocketAddress(2);
        Map<InetSocketAddress, CassandraClientPoolingContainer> pools = ImmutableMap.of(
                new InetSocketAddress(0), createMockClientPoolingContainerWithUtilization(10),
                new InetSocketAddress(1), createMockClientPoolingContainerWithUtilization(10),
                lowActivityHost, createMockClientPoolingContainerWithUtilization(0));

        NavigableMap<Integer, InetSocketAddress> result = CassandraClientPoolImpl.WeightedHosts.create(pools).hosts;

        int largestWeight = result.firstEntry().getKey();
        InetSocketAddress hostWithLargestWeight = result.firstEntry().getValue();
        int prevKey = 0;
        for (Map.Entry<Integer, InetSocketAddress> entry : result.entrySet()) {
            int currWeight = entry.getKey() - prevKey;
            prevKey = entry.getKey();
            if (currWeight > largestWeight) {
                largestWeight = currWeight;
                hostWithLargestWeight = entry.getValue();
            }
        }
        assertEquals(lowActivityHost, hostWithLargestWeight);
    }

    @Test
    public void testWeightedHostsWithMaxActivityPool() {
        InetSocketAddress highActivityHost = new InetSocketAddress(2);
        Map<InetSocketAddress, CassandraClientPoolingContainer> pools = ImmutableMap.of(
                new InetSocketAddress(0), createMockClientPoolingContainerWithUtilization(5),
                new InetSocketAddress(1), createMockClientPoolingContainerWithUtilization(5),
                highActivityHost, createMockClientPoolingContainerWithUtilization(20));

        NavigableMap<Integer, InetSocketAddress> result = CassandraClientPoolImpl.WeightedHosts.create(pools).hosts;

        int smallestWeight = result.firstEntry().getKey();
        InetSocketAddress hostWithSmallestWeight = result.firstEntry().getValue();
        int prevKey = 0;
        for (Map.Entry<Integer, InetSocketAddress> entry : result.entrySet()) {
            int currWeight = entry.getKey() - prevKey;
            prevKey = entry.getKey();
            if (currWeight < smallestWeight) {
                smallestWeight = currWeight;
                hostWithSmallestWeight = entry.getValue();
            }
        }
        assertEquals(highActivityHost, hostWithSmallestWeight);
    }

    @Test
    public void testWeightedHostsWithNonZeroWeights() {
        Map<InetSocketAddress, CassandraClientPoolingContainer> pools = ImmutableMap.of(
                new InetSocketAddress(0), createMockClientPoolingContainerWithUtilization(5),
                new InetSocketAddress(1), createMockClientPoolingContainerWithUtilization(10),
                new InetSocketAddress(2), createMockClientPoolingContainerWithUtilization(15));

        NavigableMap<Integer, InetSocketAddress> result = CassandraClientPoolImpl.WeightedHosts.create(pools).hosts;

        int prevKey = 0;
        for (Map.Entry<Integer, InetSocketAddress> entry : result.entrySet()) {
            int currWeight = entry.getKey() - prevKey;
            assertThat(currWeight, Matchers.greaterThan(0));
            prevKey = entry.getKey();
        }
    }

    // Covers a bug where we used ceilingEntry instead of higherEntry
    @Test
    public void testSelectingHostFromWeightedHostsMatchesWeight() {
        Map<InetSocketAddress, CassandraClientPoolingContainer> pools = ImmutableMap.of(
                new InetSocketAddress(0), createMockClientPoolingContainerWithUtilization(5),
                new InetSocketAddress(1), createMockClientPoolingContainerWithUtilization(10),
                new InetSocketAddress(2), createMockClientPoolingContainerWithUtilization(15));
        CassandraClientPoolImpl.WeightedHosts weightedHosts = CassandraClientPoolImpl.WeightedHosts.create(pools);
        Map<InetSocketAddress, Integer> hostsToWeight = new HashMap<>();
        int prevKey = 0;
        for (Map.Entry<Integer, InetSocketAddress> entry : weightedHosts.hosts.entrySet()) {
            hostsToWeight.put(entry.getValue(), entry.getKey() - prevKey);
            prevKey = entry.getKey();
        }

        // Exhaustively test all indexes
        Map<InetSocketAddress, Integer> numTimesSelected = new HashMap<>();
        for (int index = 0; index < weightedHosts.hosts.lastKey(); index++) {
            InetSocketAddress host = weightedHosts.getRandomHostInternal(index);
            if (!numTimesSelected.containsKey(host)) {
                numTimesSelected.put(host, 0);
            }
            numTimesSelected.put(host, numTimesSelected.get(host) + 1);
        }

        assertEquals(hostsToWeight, numTimesSelected);
    }

    private static CassandraClientPoolingContainer createMockClientPoolingContainerWithUtilization(int utilization) {
        CassandraClientPoolingContainer mock = Mockito.mock(CassandraClientPoolingContainer.class);
        Mockito.when(mock.getOpenRequests()).thenReturn(utilization);
        return mock;
    }
}
