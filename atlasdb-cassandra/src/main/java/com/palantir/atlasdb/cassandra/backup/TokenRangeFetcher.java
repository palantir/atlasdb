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

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.RangeSet;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraConstants;
import com.palantir.atlasdb.keyvalue.cassandra.LightweightOppToken;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

final class TokenRangeFetcher {
    private static final SafeLogger log = SafeLoggerFactory.get(TokenRangeFetcher.class);

    private static final int LONG_READ_TIMEOUT_MS = (int) TimeUnit.MINUTES.toMillis(2);
    // reduce this from default because we run RepairTableTask across N keyspaces at the same time
    private static final int SELECT_FETCH_SIZE = 1_000;

    private final CqlSession cqlSession;
    private final CqlMetadata cqlMetadata;
    private final CassandraKeyValueServiceConfig config;

    public TokenRangeFetcher(CqlSession cqlSession, CassandraKeyValueServiceConfig config) {
        this.config = config;
        this.cqlSession = cqlSession;
        this.cqlMetadata = cqlSession.getMetadata();
    }

    public Map<InetSocketAddress, RangeSet<LightweightOppToken>> getTokenRange(String tableName) {
        String keyspaceName = config.getKeyspaceOrThrow();
        KeyspaceMetadata keyspace = cqlMetadata.getKeyspaceMetadata(keyspaceName);
        TableMetadata tableMetadata = keyspace.getTable(tableName);
        Set<LightweightOppToken> partitionTokens = getPartitionTokens(tableMetadata);
        Map<InetSocketAddress, RangeSet<LightweightOppToken>> tokenRangesByNode = ClusterMetadataUtils.getTokenMapping(
                CassandraServersConfigs.getCqlHosts(config), cqlMetadata, keyspaceName, partitionTokens);

        if (!partitionTokens.isEmpty() && log.isDebugEnabled()) {
            int numTokenRanges = tokenRangesByNode.values().stream()
                    .mapToInt(ranges -> ranges.asRanges().size())
                    .sum();

            log.debug(
                    "Identified token ranges requiring repair",
                    SafeArg.of("keyspace", keyspace),
                    SafeArg.of("table", tableName),
                    SafeArg.of("numPartitionKeys", partitionTokens.size()),
                    SafeArg.of("numTokenRanges", numTokenRanges));
        }

        return tokenRangesByNode;
    }

    private Set<LightweightOppToken> getPartitionTokens(TableMetadata tableMetadata) {
        return cqlSession.retrieveRowKeysAtConsistencyAll(ImmutableList.of(selectDistinctRowKeys(tableMetadata)));
    }

    private static Statement selectDistinctRowKeys(TableMetadata table) {
        return QueryBuilder.select(CassandraConstants.ROW)
                // only returns the column that we need instead of all of them, otherwise we get a timeout
                .distinct()
                .from(table)
                .setConsistencyLevel(ConsistencyLevel.ALL)
                .setFetchSize(SELECT_FETCH_SIZE)
                .setReadTimeoutMillis(LONG_READ_TIMEOUT_MS);
    }
}
