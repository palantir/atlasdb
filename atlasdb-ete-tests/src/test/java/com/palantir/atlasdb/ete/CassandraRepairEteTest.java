/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.ete;

import static org.assertj.core.api.Assertions.assertThat;

import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.TokenRange;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.backup.CassandraRepairHelper;
import com.palantir.atlasdb.cassandra.backup.ClusterMetadataUtils;
import com.palantir.atlasdb.cassandra.backup.CqlCluster;
import com.palantir.atlasdb.cassandra.backup.LightweightOppTokenRange;
import com.palantir.atlasdb.containers.ThreeNodeCassandraCluster;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.Blacklist;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServiceImpl;
import com.palantir.atlasdb.keyvalue.cassandra.LightweightOppToken;
import com.palantir.atlasdb.keyvalue.cassandra.async.client.creation.ClusterFactory;
import com.palantir.atlasdb.keyvalue.cassandra.pool.CassandraClientPoolMetrics;
import com.palantir.atlasdb.keyvalue.cassandra.pool.CassandraService;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.common.streams.KeyedStream;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Predicate;
import javax.xml.bind.DatatypeConverter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CassandraRepairEteTest {
    private static final byte[] FIRST_COLUMN = PtBytes.toBytes("col1");
    private static final Cell NONEMPTY_CELL = Cell.create(PtBytes.toBytes("nonempty"), FIRST_COLUMN);
    private static final byte[] CONTENTS = PtBytes.toBytes("default_value");
    private static final String NAMESPACE = "ns";
    private static final String TABLE_1 = "table1";
    private static final TableReference TABLE_REF =
            TableReference.create(com.palantir.atlasdb.keyvalue.api.Namespace.create(NAMESPACE), TABLE_1);

    private CassandraRepairHelper cassandraRepairHelper;
    private CassandraKeyValueService kvs;
    private CassandraKeyValueServiceConfig config;
    private CqlCluster cqlCluster;
    private Cluster cluster;

    @Before
    public void setUp() {
        MetricsManager metricsManager =
                new MetricsManager(new MetricRegistry(), new DefaultTaggedMetricRegistry(), _unused -> true);

        config = ThreeNodeCassandraCluster.getKvsConfig(2);
        kvs = CassandraKeyValueServiceImpl.createForTesting(config);

        kvs.createTable(TABLE_REF, AtlasDbConstants.GENERIC_TABLE_METADATA);
        kvs.putUnlessExists(TABLE_REF, ImmutableMap.of(NONEMPTY_CELL, CONTENTS));

        cassandraRepairHelper = new CassandraRepairHelper(metricsManager, _unused -> config, _unused -> kvs);
        cluster = new ClusterFactory(Cluster::builder).constructCluster(config);
        cqlCluster = new CqlCluster(cluster, config);
    }

    @After
    public void tearDown() {
        kvs.dropTable(TABLE_REF);
    }

    @Test
    public void shouldGetRangesForBothReplicas() {
        Map<InetSocketAddress, Set<LightweightOppTokenRange>> ranges =
                cassandraRepairHelper.getRangesToRepair(cqlCluster, Namespace.of(NAMESPACE), TABLE_1);
        assertThat(ranges).hasSize(2);
    }

    @Test
    public void tokenRangesToRepairShouldBeSubsetsOfTokenMap() {
        Map<InetSocketAddress, Set<LightweightOppTokenRange>> fullTokenMap = getFullTokenMap();
        Map<InetSocketAddress, Set<LightweightOppTokenRange>> rangesToRepair =
                cassandraRepairHelper.getRangesToRepair(cqlCluster, Namespace.of(NAMESPACE), TABLE_1);

        KeyedStream.stream(rangesToRepair)
                .forEach((address, cqlRangesForHost) ->
                        assertRangesToRepairAreSubsetsOfRangesFromTokenMap(fullTokenMap, address, cqlRangesForHost));
    }

    @Test
    public void testGetTokenRanges() {
        Metadata metadata = cluster.getMetadata();

        Token token1 = metadata.newToken(ByteBuffer.wrap(DatatypeConverter.parseHexBinary("10")));
        SortedMap<Token, TokenRange> tokenRangesByEnd = KeyedStream.of(metadata.getTokenRanges())
                .mapKeys(TokenRange::getEnd)
                .collectTo(TreeMap::new);
        Token partitionKeyToken = metadata.newToken(ByteBuffer.wrap(DatatypeConverter.parseHexBinary("20")));

        Set<TokenRange> tokenRanges = ClusterMetadataUtils.getMinimalSetOfRangesForTokens(
                metadata, ImmutableSet.of(partitionKeyToken), tokenRangesByEnd);
        assertThat(tokenRanges).hasSize(1);
        TokenRange onlyRange = tokenRanges.iterator().next();
        assertThat(onlyRange.getStart()).isEqualTo(token1);
        assertThat(onlyRange.getEnd()).isEqualTo(partitionKeyToken);
    }

    // The ranges in CQL should be a subset of the Thrift ranges, except that the CQL ranges are also snipped,
    // such that if the thrift range is [5..9] but we don't have data after 7, then the CQL range will be [5..7]
    private void assertRangesToRepairAreSubsetsOfRangesFromTokenMap(
            Map<InetSocketAddress, Set<LightweightOppTokenRange>> fullTokenMap,
            InetSocketAddress address,
            Set<LightweightOppTokenRange> cqlRangesForHost) {
        String hostName = address.getHostName();
        InetSocketAddress thriftAddr = new InetSocketAddress(hostName, MultiCassandraUtils.CASSANDRA_THRIFT_PORT);
        Set<LightweightOppTokenRange> thriftRangesForHost = fullTokenMap.get(thriftAddr);
        assertThat(thriftRangesForHost).isNotNull();
        cqlRangesForHost.forEach(
                range -> assertThat(thriftRangesForHost.stream().anyMatch(isSubsetOf(range)))
                        .isTrue());
    }

    private Predicate<LightweightOppTokenRange> isSubsetOf(LightweightOppTokenRange range) {
        return thriftRange -> {
            System.out.println("Comparing range:" + range.left() + "->" + range.right());
            System.out.println("To thrift range:" + thriftRange.left() + "->" + thriftRange.right());
            return thriftRange.left().equals(range.left())
                    && thriftRange
                            .right()
                            .map(right -> right.compareTo(range.right().orElseThrow()) >= 0)
                            .orElseGet(() -> range.right().isEmpty());
        };
    }

    private Map<InetSocketAddress, Set<LightweightOppTokenRange>> getFullTokenMap() {
        CassandraService cassandraService = CassandraService.createInitialised(
                MetricsManagers.createForTests(),
                config,
                new Blacklist(config),
                new CassandraClientPoolMetrics(MetricsManagers.createForTests()));
        return invert(cassandraService.getTokenMap());
    }

    @SuppressWarnings("UnstableApiUsage")
    private Map<InetSocketAddress, Set<LightweightOppTokenRange>> invert(
            RangeMap<LightweightOppToken, List<InetSocketAddress>> tokenMap) {
        Map<InetSocketAddress, Set<LightweightOppTokenRange>> invertedMap = new HashMap<>();
        tokenMap.asMapOfRanges()
                .forEach((range, addresses) -> addresses.forEach(addr -> {
                    Set<LightweightOppTokenRange> existingRanges = invertedMap.getOrDefault(addr, new HashSet<>());
                    existingRanges.add(toTokenRange(range));
                    invertedMap.put(addr, existingRanges);
                }));

        return invertedMap;
    }

    private LightweightOppTokenRange toTokenRange(Range<LightweightOppToken> range) {
        LightweightOppTokenRange.Builder rangeBuilder = LightweightOppTokenRange.builder();
        if (range.hasLowerBound()) {
            rangeBuilder.left(range.lowerEndpoint());
        }
        if (range.hasUpperBound()) {
            rangeBuilder.right(range.upperEndpoint());
        }
        return rangeBuilder.build();
    }
}
