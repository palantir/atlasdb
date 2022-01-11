/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.cassandra.backup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs;
import com.palantir.atlasdb.cassandra.ImmutableCqlCapableConfig;
import com.palantir.atlasdb.cassandra.backup.transaction.Transactions1TableInteraction;
import com.palantir.atlasdb.cassandra.backup.transaction.Transactions2TableInteraction;
import com.palantir.atlasdb.cassandra.backup.transaction.Transactions3TableInteraction;
import com.palantir.atlasdb.cassandra.backup.transaction.TransactionsTableInteraction;
import com.palantir.atlasdb.keyvalue.cassandra.LightweightOppToken;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.timestamp.FullyBoundedTimestampRange;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

// TODO(gs): deduplicate mockery
@SuppressWarnings("UnstableApiUsage")
@RunWith(MockitoJUnitRunner.class)
public class RepairRangeFetcherTest {
    private static final InetSocketAddress HOST_1 = new InetSocketAddress("cassandra-1", 9042);
    private static final InetSocketAddress HOST_2 = new InetSocketAddress("cassandra-2", 9042);
    private static final InetSocketAddress HOST_3 = new InetSocketAddress("cassandra-3", 9042);
    private static final ImmutableList<InetSocketAddress> HOSTS = ImmutableList.of(HOST_1, HOST_2, HOST_3);
    private static final String KEYSPACE_NAME = "keyspace";
    private static final DefaultRetryPolicy POLICY = DefaultRetryPolicy.INSTANCE;
    private static final String TXN_1 = TransactionConstants.TRANSACTION_TABLE.getTableName();
    private static final String TXN_2 = TransactionConstants.TRANSACTIONS2_TABLE.getTableName();

    private static final LightweightOppToken TOKEN_1 = new LightweightOppToken("1111".getBytes(StandardCharsets.UTF_8));
    private static final LightweightOppToken TOKEN_2 = new LightweightOppToken("2222".getBytes(StandardCharsets.UTF_8));
    private static final LightweightOppToken TOKEN_3 = new LightweightOppToken("3333".getBytes(StandardCharsets.UTF_8));

    private static final Range<LightweightOppToken> RANGE_AT_MOST_1 = Range.atMost(TOKEN_1);
    private static final Range<LightweightOppToken> RANGE_1_TO_2 = Range.openClosed(TOKEN_1, TOKEN_2);
    private static final Range<LightweightOppToken> RANGE_2_TO_3 = Range.openClosed(TOKEN_2, TOKEN_3);
    private static final Range<LightweightOppToken> RANGE_GREATER_THAN_3 = Range.greaterThan(TOKEN_3);

    @Mock
    private CqlSession cqlSession;

    @Mock
    private CqlMetadata cqlMetadata;

    @Mock
    private CassandraKeyValueServiceConfig config;

    private RepairRangeFetcher repairRangeFetcher;

    @Before
    public void setUp() {
        KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
        when(keyspaceMetadata.getName()).thenReturn(KEYSPACE_NAME);

        TableMetadata txn1Metadata = mock(TableMetadata.class);
        when(txn1Metadata.getKeyspace()).thenReturn(keyspaceMetadata);
        when(txn1Metadata.getName()).thenReturn(TXN_1);

        TableMetadata txn2Metadata = mock(TableMetadata.class);
        when(txn2Metadata.getKeyspace()).thenReturn(keyspaceMetadata);
        when(txn2Metadata.getName()).thenReturn(TXN_2);

        when(keyspaceMetadata.getTables()).thenReturn(ImmutableList.of(txn1Metadata, txn2Metadata));
        when(cqlMetadata.getKeyspaceMetadata(KEYSPACE_NAME)).thenReturn(keyspaceMetadata);

        when(cqlSession.retrieveRowKeysAtConsistencyAll(anyList()))
                .thenReturn(ImmutableSet.of(token("1111"), token("5555")));

        when(cqlMetadata.getTokenRanges())
                .thenReturn(ImmutableSet.of(RANGE_AT_MOST_1, RANGE_1_TO_2, RANGE_2_TO_3, RANGE_GREATER_THAN_3));
        when(cqlMetadata.getReplicas(eq(KEYSPACE_NAME), any())).thenReturn(ImmutableSet.copyOf(HOSTS));
        when(cqlSession.getMetadata()).thenReturn(cqlMetadata);

        CassandraServersConfigs.CqlCapableConfig cqlCapableConfig = ImmutableCqlCapableConfig.builder()
                .addAllCqlHosts(HOSTS)
                .addAllThriftHosts(HOSTS)
                .build();
        when(config.servers()).thenReturn(cqlCapableConfig);
        when(config.getKeyspaceOrThrow()).thenReturn(KEYSPACE_NAME);

        repairRangeFetcher = new RepairRangeFetcher(cqlSession, config);
    }

    // TODO(gs): add tests that show we get the right token ranges
    @Test
    public void testRepairOnlyTxn1() {
        List<TransactionsTableInteraction> interactions =
                ImmutableList.of(new Transactions1TableInteraction(range(1L, 10_000_000L), POLICY));

        Map<String, Map<InetSocketAddress, RangeSet<LightweightOppToken>>> rangesForRepair =
                repairRangeFetcher.getTransactionTableRangesForRepair(interactions);
        assertThat(rangesForRepair.keySet()).containsExactly(TXN_1);
    }

    @Test
    public void testRepairOnlyTxn2() {
        List<TransactionsTableInteraction> interactions =
                ImmutableList.of(new Transactions2TableInteraction(range(1L, 10_000_000L), POLICY));
        Map<String, Map<InetSocketAddress, RangeSet<LightweightOppToken>>> rangesForRepair =
                repairRangeFetcher.getTransactionTableRangesForRepair(interactions);
        assertThat(rangesForRepair.keySet()).containsExactly(TXN_2);
    }

    @Test
    public void testRepairTxn3() {
        List<TransactionsTableInteraction> interactions =
                ImmutableList.of(new Transactions3TableInteraction(range(1L, 10_000_000L), POLICY));
        Map<String, Map<InetSocketAddress, RangeSet<LightweightOppToken>>> rangesForRepair =
                repairRangeFetcher.getTransactionTableRangesForRepair(interactions);

        // Transactions3 is backed by Transactions2 under the hood, so this is the table that will be repaired.
        assertThat(rangesForRepair.keySet()).containsExactly(TXN_2);
    }

    @Test
    public void testRepairBothTxnTables() {
        List<TransactionsTableInteraction> interactions = ImmutableList.of(
                new Transactions1TableInteraction(range(1L, 5L), POLICY),
                new Transactions2TableInteraction(range(6L, 10L), POLICY));

        Map<String, Map<InetSocketAddress, RangeSet<LightweightOppToken>>> rangesForRepair =
                repairRangeFetcher.getTransactionTableRangesForRepair(interactions);
        assertThat(rangesForRepair.keySet()).containsExactlyInAnyOrder(TXN_1, TXN_2);
    }

    @Test
    public void testRepairsEachTableOnceOnly() {
        List<TransactionsTableInteraction> interactions = ImmutableList.of(
                new Transactions1TableInteraction(range(1L, 5L), POLICY),
                new Transactions2TableInteraction(range(6L, 10L), POLICY),
                new Transactions1TableInteraction(range(11L, 15L), POLICY),
                new Transactions2TableInteraction(range(16L, 20L), POLICY));

        Map<String, Map<InetSocketAddress, RangeSet<LightweightOppToken>>> rangesForRepair =
                repairRangeFetcher.getTransactionTableRangesForRepair(interactions);
        assertThat(rangesForRepair.keySet()).containsExactlyInAnyOrder(TXN_1, TXN_2);
    }

    @Test
    public void testRepairGivesAllReplicas() {
        List<TransactionsTableInteraction> interactions =
                ImmutableList.of(new Transactions2TableInteraction(range(1L, 10_000_000L), POLICY));
        Map<String, Map<InetSocketAddress, RangeSet<LightweightOppToken>>> rangesForRepair =
                repairRangeFetcher.getTransactionTableRangesForRepair(interactions);

        assertThat(rangesForRepair.get(TXN_2).keySet()).containsExactlyInAnyOrder(HOST_1, HOST_2, HOST_3);
    }

    @Test
    public void testRepairGivesTwoReplicasForRF2() {
        when(cqlMetadata.getReplicas(eq(KEYSPACE_NAME), any())).thenReturn(ImmutableSet.of(HOST_1, HOST_2));
        List<TransactionsTableInteraction> interactions =
                ImmutableList.of(new Transactions2TableInteraction(range(1L, 10_000_000L), POLICY));
        Map<String, Map<InetSocketAddress, RangeSet<LightweightOppToken>>> rangesForRepair =
                repairRangeFetcher.getTransactionTableRangesForRepair(interactions);

        assertThat(rangesForRepair.get(TXN_2).keySet()).containsExactlyInAnyOrder(HOST_1, HOST_2);
    }

    private static FullyBoundedTimestampRange range(long lower, long upper) {
        return FullyBoundedTimestampRange.of(Range.closed(lower, upper));
    }

    // TODO(gs): refine/unify test token generation?
    private static LightweightOppToken token(String hexString) {
        return new LightweightOppToken(hexString.getBytes(StandardCharsets.UTF_8));
    }
}
