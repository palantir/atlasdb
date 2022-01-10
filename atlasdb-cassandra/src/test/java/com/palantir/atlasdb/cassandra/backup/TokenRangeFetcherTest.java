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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs;
import com.palantir.atlasdb.cassandra.ImmutableCqlCapableConfig;
import com.palantir.atlasdb.keyvalue.cassandra.LightweightOppToken;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings({"DnsLookup", "UnstableApiUsage"})
public class TokenRangeFetcherTest {
    private static final InetSocketAddress HOST_1 = new InetSocketAddress("cassandra-1", 9042);
    private static final InetSocketAddress HOST_2 = new InetSocketAddress("cassandra-2", 9042);
    private static final InetSocketAddress HOST_3 = new InetSocketAddress("cassandra-3", 9042);
    private static final ImmutableList<InetSocketAddress> HOSTS = ImmutableList.of(HOST_1, HOST_2, HOST_3);
    private static final String KEYSPACE_NAME = "keyspace";
    private static final String TABLE_NAME = "table";

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

    private TokenRangeFetcher tokenRangeFetcher;

    @Before
    public void setUp() {
        TableMetadata tableMetadata = mock(TableMetadata.class);

        KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
        when(keyspaceMetadata.getName()).thenReturn(KEYSPACE_NAME);
        when(keyspaceMetadata.getTable(TABLE_NAME)).thenReturn(tableMetadata);
        when(tableMetadata.getKeyspace()).thenReturn(keyspaceMetadata);
        when(tableMetadata.getName()).thenReturn(TABLE_NAME);
        when(cqlMetadata.getKeyspaceMetadata(KEYSPACE_NAME)).thenReturn(keyspaceMetadata);

        when(cqlSession.retrieveRowKeysAtConsistencyAll(anyList()))
                .thenReturn(ImmutableSet.of(TOKEN_1, TOKEN_2, TOKEN_3));
        when(cqlMetadata.getTokenRanges())
                .thenReturn(ImmutableSet.of(RANGE_AT_MOST_1, RANGE_1_TO_2, RANGE_2_TO_3, RANGE_GREATER_THAN_3));
        when(cqlSession.getMetadata()).thenReturn(cqlMetadata);

        CassandraServersConfigs.CqlCapableConfig cqlCapableConfig = ImmutableCqlCapableConfig.builder()
                .addAllCqlHosts(HOSTS)
                .addAllThriftHosts(HOSTS)
                .build();
        when(config.servers()).thenReturn(cqlCapableConfig);
        when(config.getKeyspaceOrThrow()).thenReturn(KEYSPACE_NAME);

        tokenRangeFetcher = new TokenRangeFetcher(cqlSession, config);
    }

    // TODO(gs): takes a long time. Need to mock more?
    @Test
    public void allNodesGetAllRangesWithThreeNodesAndRF3() {
        when(cqlMetadata.getReplicas(eq(KEYSPACE_NAME), any())).thenReturn(ImmutableSet.copyOf(HOSTS));

        Map<InetSocketAddress, RangeSet<LightweightOppToken>> tokenRange = tokenRangeFetcher.getTokenRange(TABLE_NAME);
        assertThat(tokenRange.keySet()).containsExactlyInAnyOrder(HOST_1, HOST_2, HOST_3);
        HOSTS.forEach(host -> assertThat(tokenRange.get(host)).isEqualTo(ImmutableRangeSet.of(Range.all())));
    }

    @Test
    public void testGetTokenRangesWithRF2() {
        when(cqlMetadata.getReplicas(KEYSPACE_NAME, RANGE_AT_MOST_1)).thenReturn(ImmutableSet.of(HOST_3, HOST_1));
        when(cqlMetadata.getReplicas(KEYSPACE_NAME, RANGE_GREATER_THAN_3)).thenReturn(ImmutableSet.of(HOST_3, HOST_1));
        when(cqlMetadata.getReplicas(KEYSPACE_NAME, RANGE_1_TO_2)).thenReturn(ImmutableSet.of(HOST_1, HOST_2));
        when(cqlMetadata.getReplicas(KEYSPACE_NAME, RANGE_2_TO_3)).thenReturn(ImmutableSet.of(HOST_2, HOST_3));

        Map<InetSocketAddress, RangeSet<LightweightOppToken>> tokenRange = tokenRangeFetcher.getTokenRange(TABLE_NAME);
        assertThat(tokenRange.keySet()).containsExactlyInAnyOrder(HOST_1, HOST_2, HOST_3);
        assertThat(tokenRange.get(HOST_1).asRanges())
                .containsExactly(Range.atMost(TOKEN_2), Range.greaterThan(TOKEN_3));
        assertThat(tokenRange.get(HOST_2).asRanges()).containsExactly(Range.openClosed(TOKEN_1, TOKEN_3));
        assertThat(tokenRange.get(HOST_3).asRanges())
                .containsExactly(Range.atMost(TOKEN_1), Range.greaterThan(TOKEN_2));
    }
}
