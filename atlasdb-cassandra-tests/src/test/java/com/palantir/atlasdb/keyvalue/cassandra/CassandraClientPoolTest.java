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
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.thrift.transport.TTransportException;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPool.LightweightOPPToken;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPool.WeightedHosts;
import com.palantir.common.base.FunctionCheckedException;

public class CassandraClientPoolTest {
    private CassandraKeyValueService kv;

    @Before
    public void setUp() {
        kv = CassandraKeyValueService.create(
                CassandraKeyValueServiceConfigManager.createSimpleManager(
                        ImmutableCassandraKeyValueServiceConfig.builder()
                                .addServers(new InetSocketAddress("localhost", 9160))
                                .poolSize(20)
                                .keyspace("atlasdb")
                                .ssl(false)
                                .replicationFactor(1)
                                .mutationBatchCount(10000)
                                .mutationBatchSizeBytes(10000000)
                                .fetchBatchCount(1000)
                                .safetyDisabled(true)
                                .autoRefreshNodes(true)
                                .build()));
        kv.initializeFromFreshInstance();
        kv.dropTable(AtlasDbConstants.TIMESTAMP_TABLE);
    }

    @After
    public void tearDown() {
        kv.teardown();
    }


    // This is a dumb test in the current test suite that has just one local Cassandra node.
    // Pretty legit test if run manually or if we go back to multi-node tests
    @Test
    public void testTokenMapping() {
        CassandraClientPool clientPool = kv.clientPool;
        Map<Range<LightweightOPPToken>, List<InetSocketAddress>> mapOfRanges = kv.clientPool.tokenMap.asMapOfRanges();

        for (Entry<Range<LightweightOPPToken>, List<InetSocketAddress>> entry : mapOfRanges.entrySet()) {
            Range<LightweightOPPToken> tokenRange = entry.getKey();
            List<InetSocketAddress> hosts = entry.getValue();

            clientPool.getRandomHostForKey("A".getBytes());

            if (tokenRange.hasLowerBound()) {
                Assert.assertTrue(hosts.contains(clientPool.getRandomHostForKey(tokenRange.lowerEndpoint().bytes)));
            }
            if (tokenRange.hasUpperBound()) {
                Assert.assertTrue(hosts.contains(clientPool.getRandomHostForKey(tokenRange.upperEndpoint().bytes)));
            }
        }
    }

    @Test
    public void testPoolGivenNoOptionTalksToBlacklistedHosts() {
        kv.clientPool.blacklistedHosts.putAll(
                Maps.transformValues(kv.clientPool.currentPools, new Function<CassandraClientPoolingContainer, Long>() {
                    @Override
                    public Long apply(CassandraClientPoolingContainer dontCare) {
                        return Long.MAX_VALUE;
                    }
                }));
        try {
            kv.clientPool.run(describeRing);
        } catch (Exception e) {
            Assert.fail("Should have been allowed to attempt forward progress after blacklisting all hosts in pool.");
        }

        kv.clientPool.blacklistedHosts.clear();
    }

    private FunctionCheckedException<Cassandra.Client, List<TokenRange>, Exception> describeRing = new FunctionCheckedException<Client, List<TokenRange>, Exception>() {
        @Override
        public List<TokenRange> apply (Cassandra.Client client) throws Exception {
            return client.describe_ring("atlasdb");
        }};

    @Test
    public void testIsConnectionException() {
        Assert.assertFalse(CassandraClientPool.isConnectionException(new TimedOutException()));
        Assert.assertFalse(CassandraClientPool.isConnectionException(new TTransportException()));
        Assert.assertTrue(CassandraClientPool.isConnectionException(new TTransportException(new SocketTimeoutException())));
    }

    @Test
    public void testIsRetriableException() {
        Assert.assertTrue(CassandraClientPool.isRetriableException(new TimedOutException()));
        Assert.assertTrue(CassandraClientPool.isRetriableException(new TTransportException()));
        Assert.assertTrue(CassandraClientPool.isRetriableException(new TTransportException(new SocketTimeoutException())));
    }

    @Test
    public void testWeightedHostsWithUniformActivity() {
        Map<InetSocketAddress, CassandraClientPoolingContainer> pools = ImmutableMap.of(
                new InetSocketAddress(0), createMockClientPoolingContainerWithUtilization(10),
                new InetSocketAddress(1), createMockClientPoolingContainerWithUtilization(10),
                new InetSocketAddress(2), createMockClientPoolingContainerWithUtilization(10));

        TreeMap<Integer, InetSocketAddress> result = CassandraClientPool.WeightedHosts.create(pools).hosts;

        int expectedWeight = result.firstEntry().getKey();
        int prevKey = 0;
        for (Map.Entry<Integer, InetSocketAddress> entry : result.entrySet()) {
            int currWeight = entry.getKey() - prevKey;
            Assert.assertEquals(expectedWeight, currWeight);
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

        TreeMap<Integer, InetSocketAddress> result = CassandraClientPool.WeightedHosts.create(pools).hosts;

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
        Assert.assertEquals(lowActivityHost, hostWithLargestWeight);
    }

    @Test
    public void testWeightedHostsWithMaxActivityPool() {
        InetSocketAddress highActivityHost = new InetSocketAddress(2);
        Map<InetSocketAddress, CassandraClientPoolingContainer> pools = ImmutableMap.of(
                new InetSocketAddress(0), createMockClientPoolingContainerWithUtilization(5),
                new InetSocketAddress(1), createMockClientPoolingContainerWithUtilization(5),
                highActivityHost, createMockClientPoolingContainerWithUtilization(20));

        TreeMap<Integer, InetSocketAddress> result = CassandraClientPool.WeightedHosts.create(pools).hosts;

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
        Assert.assertEquals(highActivityHost, hostWithSmallestWeight);
    }

    @Test
    public void testWeightedHostsWithNonZeroWeights() {
        Map<InetSocketAddress, CassandraClientPoolingContainer> pools = ImmutableMap.of(
                new InetSocketAddress(0), createMockClientPoolingContainerWithUtilization(5),
                new InetSocketAddress(1), createMockClientPoolingContainerWithUtilization(10),
                new InetSocketAddress(2), createMockClientPoolingContainerWithUtilization(15));

        TreeMap<Integer, InetSocketAddress> result = CassandraClientPool.WeightedHosts.create(pools).hosts;

        int prevKey = 0;
        for (Map.Entry<Integer, InetSocketAddress> entry : result.entrySet()) {
            int currWeight = entry.getKey() - prevKey;
            Assert.assertThat(currWeight, Matchers.greaterThan(0));
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
        WeightedHosts weightedHosts = WeightedHosts.create(pools);
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

        Assert.assertEquals(hostsToWeight, numTimesSelected);
    }

    private static CassandraClientPoolingContainer createMockClientPoolingContainerWithUtilization(int utilization) {
        CassandraClientPoolingContainer mock = Mockito.mock(CassandraClientPoolingContainer.class);
        Mockito.when(mock.getPoolUtilization()).thenReturn(utilization);
        return mock;
    }
}
