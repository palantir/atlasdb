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
import static org.mockito.Mockito.when;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.backup.transaction.Transactions1TableInteraction;
import com.palantir.atlasdb.cassandra.backup.transaction.Transactions2TableInteraction;
import com.palantir.atlasdb.cassandra.backup.transaction.Transactions3TableInteraction;
import com.palantir.atlasdb.cassandra.backup.transaction.TransactionsTableInteraction;
import com.palantir.atlasdb.keyvalue.cassandra.LightweightOppToken;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.timestamp.FullyBoundedTimestampRange;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("UnstableApiUsage")
@RunWith(MockitoJUnitRunner.class)
public class RepairRangeFetcherTest {
    static final LightweightOppToken OTHER_TOKEN = BackupTestUtils.lightweightOppToken("7777");
    static final DefaultRetryPolicy POLICY = DefaultRetryPolicy.INSTANCE;
    static final String TXN_1 = TransactionConstants.TRANSACTION_TABLE.getTableName();
    static final String TXN_2 = TransactionConstants.TRANSACTIONS2_TABLE.getTableName();

    @Mock
    private CqlSession cqlSession;

    @Mock
    private CqlMetadata cqlMetadata;

    @Mock
    private CassandraKeyValueServiceConfig config;

    private RepairRangeFetcher repairRangeFetcher;

    @Before
    public void setUp() {
        KeyspaceMetadata keyspaceMetadata = BackupTestUtils.mockKeyspaceMetadata(cqlMetadata);
        List<TableMetadata> tableMetadatas = BackupTestUtils.mockTableMetadatas(keyspaceMetadata, TXN_1, TXN_2);
        when(keyspaceMetadata.getTables()).thenReturn(tableMetadatas);

        BackupTestUtils.mockTokenRanges(cqlSession, cqlMetadata);
        BackupTestUtils.mockConfig(config);

        when(cqlSession.retrieveRowKeysAtConsistencyAll(anyList()))
                .thenReturn(ImmutableSet.of(BackupTestUtils.TOKEN_1, OTHER_TOKEN));
        when(cqlMetadata.getReplicas(eq(BackupTestUtils.KEYSPACE_NAME), any()))
                .thenReturn(ImmutableSet.copyOf(BackupTestUtils.HOSTS));

        repairRangeFetcher = new RepairRangeFetcher(cqlSession, config);
    }

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
                ImmutableList.of(new Transactions3TableInteraction(range(1L, 10_000_000L)));
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
    public void testRepairGivesCorrectTokenRanges() {
        List<TransactionsTableInteraction> interactions =
                ImmutableList.of(new Transactions2TableInteraction(range(1L, 10_000_000L), POLICY));
        Map<String, Map<InetSocketAddress, RangeSet<LightweightOppToken>>> rangesForRepair =
                repairRangeFetcher.getTransactionTableRangesForRepair(interactions);

        assertThat(rangesForRepair.get(TXN_2).get(BackupTestUtils.HOST_1).asRanges())
                .containsExactlyInAnyOrder(
                        Range.atMost(BackupTestUtils.TOKEN_1),
                        Range.openClosed(BackupTestUtils.TOKEN_2, OTHER_TOKEN),
                        Range.greaterThan(BackupTestUtils.TOKEN_3));
    }

    @Test
    public void testRepairGivesAllReplicas() {
        List<TransactionsTableInteraction> interactions =
                ImmutableList.of(new Transactions2TableInteraction(range(1L, 10_000_000L), POLICY));
        Map<String, Map<InetSocketAddress, RangeSet<LightweightOppToken>>> rangesForRepair =
                repairRangeFetcher.getTransactionTableRangesForRepair(interactions);

        assertThat(rangesForRepair.get(TXN_2).keySet())
                .containsExactlyInAnyOrder(BackupTestUtils.HOST_1, BackupTestUtils.HOST_2, BackupTestUtils.HOST_3);
    }

    @Test
    public void testRepairGivesTwoReplicasForRF2() {
        when(cqlMetadata.getReplicas(eq(BackupTestUtils.KEYSPACE_NAME), any()))
                .thenReturn(ImmutableSet.of(BackupTestUtils.HOST_1, BackupTestUtils.HOST_2));
        List<TransactionsTableInteraction> interactions =
                ImmutableList.of(new Transactions2TableInteraction(range(1L, 10_000_000L), POLICY));
        Map<String, Map<InetSocketAddress, RangeSet<LightweightOppToken>>> rangesForRepair =
                repairRangeFetcher.getTransactionTableRangesForRepair(interactions);

        assertThat(rangesForRepair.get(TXN_2).keySet())
                .containsExactlyInAnyOrder(BackupTestUtils.HOST_1, BackupTestUtils.HOST_2);
    }

    private static FullyBoundedTimestampRange range(long lower, long upper) {
        return FullyBoundedTimestampRange.of(Range.closed(lower, upper));
    }
}
