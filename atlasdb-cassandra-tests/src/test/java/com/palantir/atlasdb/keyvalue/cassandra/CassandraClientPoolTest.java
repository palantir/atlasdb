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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.TokenRange;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPool.LightweightOPPToken;
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

    private FunctionCheckedException<Cassandra.Client, List<TokenRange>, Exception> describeRing = new FunctionCheckedException<Cassandra.Client, List<TokenRange>, Exception>() {
        @Override
        public List<TokenRange> apply (Cassandra.Client client) throws Exception {
            return client.describe_ring("atlasdb");
        }};

}
