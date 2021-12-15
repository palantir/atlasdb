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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.TokenRange;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.RangeSet;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.backup.CassandraRepairHelper;
import com.palantir.atlasdb.cassandra.backup.ClusterMetadataUtils;
import com.palantir.atlasdb.cassandra.backup.CqlCluster;
import com.palantir.atlasdb.cassandra.backup.RangesForRepair;
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
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.impl.TransactionTables;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.common.streams.KeyedStream;
import com.palantir.timestamp.FullyBoundedTimestampRange;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import javax.xml.bind.DatatypeConverter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CassandraRepairEteTest {
    private static final byte[] FIRST_COLUMN = PtBytes.toBytes("col1");
    private static final Cell NONEMPTY_CELL = Cell.create(PtBytes.toBytes("nonempty"), FIRST_COLUMN);
    private static final byte[] CONTENTS = PtBytes.toBytes("default_value");
    private static final String NAMESPACE_NAME = "ns";
    private static final Namespace NAMESPACE = Namespace.of(NAMESPACE_NAME);
    private static final String TABLE_1 = "table1";
    private static final TableReference TABLE_REF =
            TableReference.create(com.palantir.atlasdb.keyvalue.api.Namespace.create(NAMESPACE_NAME), TABLE_1);

    private CassandraRepairHelper cassandraRepairHelper;
    private CassandraKeyValueService kvs;
    private CassandraKeyValueServiceConfig config;
    private CqlCluster cqlCluster;
    private Metadata metadata;
    private TreeMap<Token, TokenRange> tokenRangesByEnd;

    @Before
    public void setUp() {
        config = ThreeNodeCassandraCluster.getKvsConfig(2);
        kvs = CassandraKeyValueServiceImpl.createForTesting(config);
        TransactionTables.createTables(kvs);

        kvs.createTable(TABLE_REF, AtlasDbConstants.GENERIC_TABLE_METADATA);
        kvs.putUnlessExists(TABLE_REF, ImmutableMap.of(NONEMPTY_CELL, CONTENTS));

        cassandraRepairHelper = new CassandraRepairHelper(_unused -> config, _unused -> kvs);
        Cluster cluster = new ClusterFactory(Cluster::builder).constructCluster(config);
        cqlCluster = new CqlCluster(cluster, config);
        metadata = cluster.getMetadata();

        tokenRangesByEnd = KeyedStream.of(metadata.getTokenRanges())
                .mapKeys(TokenRange::getEnd)
                .collectTo(TreeMap::new);
    }

    @After
    public void tearDown() {
        kvs.dropTable(TABLE_REF);
    }

    @Test
    public void testRepairOnlyTxn1() {
        List<String> tablesRepaired = new ArrayList<>();
        BiConsumer<String, RangesForRepair> repairer = (table, _unused) -> tablesRepaired.add(table);

        Map<FullyBoundedTimestampRange, Integer> ranges =
                ImmutableMap.of(FullyBoundedTimestampRange.of(Range.closed(1L, 10_000_000L)), 1);
        cassandraRepairHelper.repairTransactionsTables(NAMESPACE, ranges, repairer);
        assertThat(tablesRepaired).containsExactly(TransactionConstants.TRANSACTION_TABLE.getTableName());
    }

    @Test
    public void testRepairOnlyTxn2() {
        List<String> tablesRepaired = new ArrayList<>();
        BiConsumer<String, RangesForRepair> repairer = (table, _unused) -> tablesRepaired.add(table);

        Map<FullyBoundedTimestampRange, Integer> ranges =
                ImmutableMap.of(FullyBoundedTimestampRange.of(Range.closed(1L, 10_000_000L)), 2);
        cassandraRepairHelper.repairTransactionsTables(NAMESPACE, ranges, repairer);
        assertThat(tablesRepaired).containsExactly(TransactionConstants.TRANSACTIONS2_TABLE.getTableName());
    }

    @Test
    public void testRepairBothTxnTables() {
        List<String> tablesRepaired = new ArrayList<>();
        BiConsumer<String, RangesForRepair> repairer = (table, _unused) -> tablesRepaired.add(table);

        Map<FullyBoundedTimestampRange, Integer> ranges = ImmutableMap.of(
                FullyBoundedTimestampRange.of(Range.closed(1L, 5L)), 1,
                FullyBoundedTimestampRange.of(Range.closed(6L, 10L)), 2);
        cassandraRepairHelper.repairTransactionsTables(NAMESPACE, ranges, repairer);
        assertThat(tablesRepaired)
                .containsExactlyInAnyOrder(
                        TransactionConstants.TRANSACTION_TABLE.getTableName(),
                        TransactionConstants.TRANSACTIONS2_TABLE.getTableName());
    }

    @Test
    public void testRepairsEachTableOnceOnly() {
        List<String> tablesRepaired = new ArrayList<>();
        AtomicInteger numRepairs = new AtomicInteger(0);
        BiConsumer<String, RangesForRepair> repairer = (table, _unused) -> {
            numRepairs.incrementAndGet();
            tablesRepaired.add(table);
        };

        Map<FullyBoundedTimestampRange, Integer> ranges = ImmutableMap.of(
                FullyBoundedTimestampRange.of(Range.closed(1L, 5L)), 1,
                FullyBoundedTimestampRange.of(Range.closed(6L, 10L)), 2,
                FullyBoundedTimestampRange.of(Range.closed(11L, 15L)), 1,
                FullyBoundedTimestampRange.of(Range.closed(16L, 20L)), 2);
        cassandraRepairHelper.repairTransactionsTables(NAMESPACE, ranges, repairer);
        assertThat(tablesRepaired)
                .containsExactlyInAnyOrder(
                        TransactionConstants.TRANSACTION_TABLE.getTableName(),
                        TransactionConstants.TRANSACTIONS2_TABLE.getTableName());
        assertThat(numRepairs).hasValue(2);
    }

    @Test
    public void shouldGetRangesForBothReplicas() {
        RangesForRepair ranges = cassandraRepairHelper.getRangesToRepair(cqlCluster, NAMESPACE, TABLE_1);
        assertThat(ranges.asMap()).hasSize(2);
    }

    @Test
    public void tokenRangesToRepairShouldBeSubsetsOfTokenMap() {
        Map<InetSocketAddress, Set<Range<LightweightOppToken>>> fullTokenMap = getFullTokenMap();
        RangesForRepair rangesToRepair = cassandraRepairHelper.getRangesToRepair(cqlCluster, NAMESPACE, TABLE_1);

        KeyedStream.stream(rangesToRepair.asMap())
                .forEach((address, cqlRangesForHost) ->
                        assertRangesToRepairAreSubsetsOfRangesFromTokenMap(fullTokenMap, address, cqlRangesForHost));
    }

    @Test
    public void testMinimalSetOfTokenRanges() {
        Token partitionKeyToken = getToken("9000");

        Token lastTokenBeforePartitionKey = tokenRangesByEnd.lowerKey(partitionKeyToken);

        Set<TokenRange> tokenRanges = ClusterMetadataUtils.getMinimalSetOfRangesForTokens(
                metadata, ImmutableSet.of(partitionKeyToken), tokenRangesByEnd);
        assertThat(tokenRanges).hasSize(1);
        TokenRange onlyRange = tokenRanges.iterator().next();
        assertThat(onlyRange.getStart()).isEqualTo(lastTokenBeforePartitionKey);
        assertThat(onlyRange.getEnd()).isEqualTo(partitionKeyToken);
    }

    @Test
    public void testSmallTokenRangeBeforeFirstVnode() {
        Token partitionKeyToken = getToken("0010");

        Set<TokenRange> tokenRanges = ClusterMetadataUtils.getMinimalSetOfRangesForTokens(
                metadata, ImmutableSet.of(partitionKeyToken), tokenRangesByEnd);
        assertThat(tokenRanges).hasSize(1);
        TokenRange onlyRange = tokenRanges.iterator().next();
        assertThat(onlyRange.getStart()).isEqualTo(tokenRangesByEnd.lowerKey(getToken("ff")));
        assertThat(onlyRange.getEnd()).isEqualTo(partitionKeyToken);
    }

    @Test
    public void testSmallTokenRangeOnVnode() {
        Token firstEndToken = tokenRangesByEnd.firstKey();
        Token secondEndToken = tokenRangesByEnd.higherKey(firstEndToken);
        Set<TokenRange> tokenRanges = ClusterMetadataUtils.getMinimalSetOfRangesForTokens(
                metadata, ImmutableSet.of(secondEndToken), tokenRangesByEnd);
        assertThat(tokenRanges).hasSize(1);
        TokenRange onlyRange = tokenRanges.iterator().next();
        assertThat(onlyRange.getStart()).isEqualTo(firstEndToken);
        assertThat(onlyRange.getEnd()).isEqualTo(secondEndToken);
    }

    @Test
    public void testSmallTokenRangeDedupe() {
        Token partitionKeyToken1 = getToken("9000");
        Token partitionKeyToken2 = getToken("9001");
        Set<TokenRange> tokenRanges = ClusterMetadataUtils.getMinimalSetOfRangesForTokens(
                metadata, ImmutableSet.of(partitionKeyToken1, partitionKeyToken2), tokenRangesByEnd);
        assertThat(tokenRanges).hasSize(1);
        TokenRange onlyRange = tokenRanges.iterator().next();
        assertThat(onlyRange.getStart()).isEqualTo(tokenRangesByEnd.lowerKey(partitionKeyToken1));
        assertThat(onlyRange.getEnd()).isEqualTo(partitionKeyToken2);
    }

    @Test
    public void testRemoveNestedRanges() {
        Token duplicatedStartKey = getToken("0001");
        Token nestedEndKey = getToken("000101");
        Token outerEndKey = getToken("000102");
        TokenRange nested = metadata.newTokenRange(duplicatedStartKey, nestedEndKey);
        TokenRange outer = metadata.newTokenRange(duplicatedStartKey, outerEndKey);
        assertThat(ClusterMetadataUtils.findLatestEndingRange(nested, outer)).isEqualTo(outer);
    }

    @Test
    public void testMinTokenRangeIsLatestEnding() {
        Token duplicatedStartKey = getToken("0001");
        Token normalEndKey = getToken("000101");
        TokenRange nested = metadata.newTokenRange(duplicatedStartKey, normalEndKey);
        TokenRange outer = metadata.newTokenRange(duplicatedStartKey, minToken());
        assertThat(ClusterMetadataUtils.findLatestEndingRange(nested, outer)).isEqualTo(outer);
    }

    @Test
    public void testMinTokenIsStart() {
        Token nestedEndKey = getToken("0001");
        Token outerEndKey = getToken("0002");
        TokenRange nested = metadata.newTokenRange(minToken(), nestedEndKey);
        TokenRange outer = metadata.newTokenRange(minToken(), outerEndKey);
        assertThat(ClusterMetadataUtils.findLatestEndingRange(nested, outer)).isEqualTo(outer);
    }

    @Test
    public void testRemoveNestedWraparoundRanges() {
        Token duplicatedStartKey = getToken("ff");
        Token innerWrapAround = getToken("0001");
        Token outerWrapAround = getToken("0002");
        TokenRange innerWrapAroundRange = metadata.newTokenRange(duplicatedStartKey, innerWrapAround);
        TokenRange outerWrapAroundRange = metadata.newTokenRange(duplicatedStartKey, outerWrapAround);
        assertThat(ClusterMetadataUtils.findLatestEndingRange(innerWrapAroundRange, outerWrapAroundRange))
                .isEqualTo(outerWrapAroundRange);
    }

    @Test
    public void testRemoveNestedWraparoundAndNonWrapRanges() {
        Token duplicatedStartKey = getToken("ff");
        Token nonWrapAround = getToken("ff01");
        Token wrapAround = getToken("0001");
        TokenRange nonWrapAroundRange = metadata.newTokenRange(duplicatedStartKey, nonWrapAround);
        TokenRange wrapAroundRange = metadata.newTokenRange(duplicatedStartKey, wrapAround);
        assertThat(ClusterMetadataUtils.findLatestEndingRange(nonWrapAroundRange, wrapAroundRange))
                .isEqualTo(wrapAroundRange);
    }

    // The ranges in CQL should be a subset of the Thrift ranges, except that the CQL ranges are also snipped,
    // such that if the thrift range is [5..9] but we don't have data after 7, then the CQL range will be [5..7]
    @SuppressWarnings({"DnsLookup", "ReverseDnsLookup", "UnstableApiUsage"})
    private void assertRangesToRepairAreSubsetsOfRangesFromTokenMap(
            Map<InetSocketAddress, Set<Range<LightweightOppToken>>> fullTokenMap,
            InetSocketAddress address,
            RangeSet<LightweightOppToken> cqlRangesForHost) {
        String hostName = address.getHostName();
        InetSocketAddress thriftAddr = new InetSocketAddress(hostName, MultiCassandraUtils.CASSANDRA_THRIFT_PORT);
        assertThat(fullTokenMap.get(thriftAddr)).isNotNull();
        Set<Range<LightweightOppToken>> thriftRanges = fullTokenMap.get(thriftAddr);

        cqlRangesForHost.asRanges().forEach(range -> assertThat(
                        thriftRanges.stream().anyMatch(containsEntirely(range)))
                .isTrue());
    }

    private Predicate<Range<LightweightOppToken>> containsEntirely(Range<LightweightOppToken> range) {
        return thriftRange -> safeLowerBound(thriftRange).equals(safeLowerBound(range))
                && (thriftRange.hasUpperBound()
                        ? range.hasUpperBound() && thriftRange.upperEndpoint().compareTo(range.upperEndpoint()) >= 0
                        : !range.hasUpperBound());
    }

    private Optional<LightweightOppToken> safeLowerBound(Range<LightweightOppToken> range) {
        return range.hasLowerBound() ? Optional.of(range.lowerEndpoint()) : Optional.empty();
    }

    // This needs to be Set<Range>, not a RangeSet, because we don't want to merge the token ranges for these tests.
    private Map<InetSocketAddress, Set<Range<LightweightOppToken>>> getFullTokenMap() {
        CassandraService cassandraService = CassandraService.createInitialized(
                MetricsManagers.createForTests(),
                config,
                new Blacklist(config),
                new CassandraClientPoolMetrics(MetricsManagers.createForTests()));
        return invert(cassandraService.getTokenMap());
    }

    @SuppressWarnings("UnstableApiUsage")
    private Map<InetSocketAddress, Set<Range<LightweightOppToken>>> invert(
            RangeMap<LightweightOppToken, List<InetSocketAddress>> tokenMap) {
        Map<InetSocketAddress, Set<Range<LightweightOppToken>>> invertedMap = new HashMap<>();
        tokenMap.asMapOfRanges()
                .forEach((range, addresses) -> addresses.forEach(address -> {
                    Set<Range<LightweightOppToken>> existingRanges = invertedMap.getOrDefault(address, new HashSet<>());
                    existingRanges.add(range);
                    invertedMap.put(address, existingRanges);
                }));

        return invertedMap;
    }

    private Token minToken() {
        return getToken("");
    }

    private Token getToken(String hexBinary) {
        return metadata.newToken(ByteBuffer.wrap(DatatypeConverter.parseHexBinary(hexBinary)));
    }
}
