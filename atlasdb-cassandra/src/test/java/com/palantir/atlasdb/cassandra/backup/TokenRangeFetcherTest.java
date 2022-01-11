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

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.cassandra.LightweightOppToken;
import java.net.InetSocketAddress;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings({"DnsLookup", "UnstableApiUsage"})
public class TokenRangeFetcherTest {
    private static final String TABLE_NAME = "table";

    @Mock
    private CqlSession cqlSession;

    @Mock
    private CqlMetadata cqlMetadata;

    @Mock
    private CassandraKeyValueServiceConfig config;

    private TokenRangeFetcher tokenRangeFetcher;

    @Before
    public void setUp() {
        BackupTestUtils.mockMetadata(cqlMetadata, TABLE_NAME);
        BackupTestUtils.mockConfig(config);
        BackupTestUtils.mockTokenRanges(cqlSession, cqlMetadata);

        when(cqlSession.retrieveRowKeysAtConsistencyAll(anyList()))
                .thenReturn(ImmutableSet.of(BackupTestUtils.TOKEN_1, BackupTestUtils.TOKEN_2, BackupTestUtils.TOKEN_3));

        tokenRangeFetcher = new TokenRangeFetcher(cqlSession, config);
    }

    @Test
    public void allNodesGetAllRangesWithThreeNodesAndRF3() {
        when(cqlMetadata.getReplicas(eq(BackupTestUtils.KEYSPACE_NAME), any()))
                .thenReturn(ImmutableSet.copyOf(BackupTestUtils.HOSTS));

        Map<InetSocketAddress, RangeSet<LightweightOppToken>> tokenRange = tokenRangeFetcher.getTokenRange(TABLE_NAME);
        assertThat(tokenRange.keySet())
                .containsExactlyInAnyOrder(BackupTestUtils.HOST_1, BackupTestUtils.HOST_2, BackupTestUtils.HOST_3);
        BackupTestUtils.HOSTS.forEach(
                host -> assertThat(tokenRange.get(host)).isEqualTo(ImmutableRangeSet.of(Range.all())));
    }

    @Test
    public void testGetTokenRangesWithRF2() {
        when(cqlMetadata.getReplicas(BackupTestUtils.KEYSPACE_NAME, BackupTestUtils.RANGE_AT_MOST_1))
                .thenReturn(ImmutableSet.of(BackupTestUtils.HOST_3, BackupTestUtils.HOST_1));
        when(cqlMetadata.getReplicas(BackupTestUtils.KEYSPACE_NAME, BackupTestUtils.RANGE_GREATER_THAN_3))
                .thenReturn(ImmutableSet.of(BackupTestUtils.HOST_3, BackupTestUtils.HOST_1));
        when(cqlMetadata.getReplicas(BackupTestUtils.KEYSPACE_NAME, BackupTestUtils.RANGE_1_TO_2))
                .thenReturn(ImmutableSet.of(BackupTestUtils.HOST_1, BackupTestUtils.HOST_2));
        when(cqlMetadata.getReplicas(BackupTestUtils.KEYSPACE_NAME, BackupTestUtils.RANGE_2_TO_3))
                .thenReturn(ImmutableSet.of(BackupTestUtils.HOST_2, BackupTestUtils.HOST_3));

        Map<InetSocketAddress, RangeSet<LightweightOppToken>> tokenRange = tokenRangeFetcher.getTokenRange(TABLE_NAME);
        assertThat(tokenRange.keySet())
                .containsExactlyInAnyOrder(BackupTestUtils.HOST_1, BackupTestUtils.HOST_2, BackupTestUtils.HOST_3);
        assertThat(tokenRange.get(BackupTestUtils.HOST_1).asRanges())
                .containsExactly(Range.atMost(BackupTestUtils.TOKEN_2), Range.greaterThan(BackupTestUtils.TOKEN_3));
        assertThat(tokenRange.get(BackupTestUtils.HOST_2).asRanges())
                .containsExactly(Range.openClosed(BackupTestUtils.TOKEN_1, BackupTestUtils.TOKEN_3));
        assertThat(tokenRange.get(BackupTestUtils.HOST_3).asRanges())
                .containsExactly(Range.atMost(BackupTestUtils.TOKEN_1), Range.greaterThan(BackupTestUtils.TOKEN_2));
    }
}
