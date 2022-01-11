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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs;
import com.palantir.atlasdb.cassandra.ImmutableCqlCapableConfig;
import com.palantir.atlasdb.keyvalue.cassandra.LightweightOppToken;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.xml.bind.DatatypeConverter;

@SuppressWarnings("DnsLookup")
final class BackupTestUtils {
    static final InetSocketAddress HOST_1 = new InetSocketAddress("cassandra-1", 9042);
    static final InetSocketAddress HOST_2 = new InetSocketAddress("cassandra-2", 9042);
    static final InetSocketAddress HOST_3 = new InetSocketAddress("cassandra-3", 9042);
    static final ImmutableList<InetSocketAddress> HOSTS = ImmutableList.of(HOST_1, HOST_2, HOST_3);
    static final String KEYSPACE_NAME = "keyspace";

    static final LightweightOppToken TOKEN_1 = BackupTestUtils.lightweightOppToken("1111");
    static final LightweightOppToken TOKEN_2 = BackupTestUtils.lightweightOppToken("5555");
    static final LightweightOppToken TOKEN_3 = BackupTestUtils.lightweightOppToken("9999");

    static final Range<LightweightOppToken> RANGE_AT_MOST_1 = Range.atMost(TOKEN_1);
    static final Range<LightweightOppToken> RANGE_1_TO_2 = Range.openClosed(TOKEN_1, TOKEN_2);
    static final Range<LightweightOppToken> RANGE_2_TO_3 = Range.openClosed(TOKEN_2, TOKEN_3);
    static final Range<LightweightOppToken> RANGE_GREATER_THAN_3 = Range.greaterThan(TOKEN_3);

    private BackupTestUtils() {
        // utility
    }

    static LightweightOppToken lightweightOppToken(String hexString) {
        return new LightweightOppToken(DatatypeConverter.parseHexBinary(hexString));
    }

    static void mockTokenRanges(CqlSession cqlSession, CqlMetadata cqlMetadata) {
        when(cqlMetadata.getTokenRanges())
                .thenReturn(ImmutableSet.of(RANGE_AT_MOST_1, RANGE_1_TO_2, RANGE_2_TO_3, RANGE_GREATER_THAN_3));
        when(cqlSession.getMetadata()).thenReturn(cqlMetadata);
    }

    static void mockConfig(CassandraKeyValueServiceConfig config) {
        CassandraServersConfigs.CqlCapableConfig cqlCapableConfig = ImmutableCqlCapableConfig.builder()
                .addAllCqlHosts(HOSTS)
                .addAllThriftHosts(HOSTS)
                .build();
        when(config.servers()).thenReturn(cqlCapableConfig);
        when(config.getKeyspaceOrThrow()).thenReturn(KEYSPACE_NAME);
    }

    static List<TableMetadata> mockTableMetadatas(KeyspaceMetadata keyspaceMetadata, String... tableNames) {
        List<TableMetadata> tableMetadatas = Arrays.stream(tableNames)
                .map(tableName -> mockTableMetadata(keyspaceMetadata, tableName))
                .collect(Collectors.toList());
        return tableMetadatas;
    }

    static KeyspaceMetadata mockKeyspaceMetadata(CqlMetadata cqlMetadata) {
        KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
        when(keyspaceMetadata.getName()).thenReturn(KEYSPACE_NAME);
        when(cqlMetadata.getKeyspaceMetadata(KEYSPACE_NAME)).thenReturn(keyspaceMetadata);
        return keyspaceMetadata;
    }

    private static TableMetadata mockTableMetadata(KeyspaceMetadata keyspaceMetadata, String tableName) {
        TableMetadata tableMetadata = mock(TableMetadata.class);
        when(tableMetadata.getKeyspace()).thenReturn(keyspaceMetadata);
        when(tableMetadata.getName()).thenReturn(tableName);
        return tableMetadata;
    }
}
