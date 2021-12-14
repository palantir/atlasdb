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

package com.palantir.atlasdb.cassandra.backup;

import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.TokenRange;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.utils.Bytes;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.backup.transaction.TransactionsTableInteraction;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.LightweightOppToken;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueService;
import com.palantir.atlasdb.schema.TargetedSweepTables;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.timestamp.FullyBoundedTimestampRange;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CassandraRepairHelper {
    private static final SafeLogger log = SafeLoggerFactory.get(CassandraRepairHelper.class);

    private static final Set<TableReference> TABLES_TO_REPAIR =
            Sets.union(ImmutableSet.of(AtlasDbConstants.COORDINATION_TABLE), TargetedSweepTables.REPAIR_ON_RESTORE);

    private final Function<Namespace, CassandraKeyValueServiceConfig> keyValueServiceConfigFactory;
    private final Function<Namespace, KeyValueService> keyValueServiceFactory;

    public CassandraRepairHelper(
            Function<Namespace, CassandraKeyValueServiceConfig> keyValueServiceConfigFactory,
            Function<Namespace, KeyValueService> keyValueServiceFactory) {
        this.keyValueServiceConfigFactory = keyValueServiceConfigFactory;
        this.keyValueServiceFactory = keyValueServiceFactory;
    }

    public void repairInternalTables(Namespace namespace, Consumer<RangesForRepair> repairTable) {
        KeyValueService kvs = keyValueServiceFactory.apply(namespace);
        CqlCluster cqlCluster = getCqlCluster(namespace);
        kvs.getAllTableNames().stream()
                .filter(TABLES_TO_REPAIR::contains)
                .map(TableReference::getTableName)
                .map(tableName -> getRangesToRepair(cqlCluster, namespace, tableName))
                // TODO(gs): this will do repairs serially, instead of batched. Is this fine? Port batching from
                //   internal product?
                .forEach(repairTable);
    }

    public void repairTransactionsTables(
            Namespace namespace,
            Map<FullyBoundedTimestampRange, Integer> coordinationMap,
            Consumer<RangesForRepair> repairTable) {
        List<TransactionsTableInteraction> transactionsTableInteractions =
                TransactionsTableInteraction.getTransactionTableInteractions(
                        coordinationMap, DefaultRetryPolicy.INSTANCE);

        Map<String, RangesForRepair> tokenRangesForRepair =
                getRangesForRepairByTable(namespace, transactionsTableInteractions);

        // 5. repair ranges
        KeyedStream.stream(tokenRangesForRepair).forEach((table, ranges) -> {
            log.info("Repairing ranges for table", SafeArg.of("table", table));
            repairTable.accept(ranges);
        });
    }

    private Map<String, RangesForRepair> getRangesForRepairByTable(
            Namespace namespace, List<TransactionsTableInteraction> transactionsTableInteractions) {
        // 3. get partition tokens
        Map<String, Set<Token>> partitionKeysByTable =
                getPartitionKeysByTable(namespace, transactionsTableInteractions);

        if (log.isDebugEnabled()) {
            Map<String, FullyBoundedTimestampRange> loggableTableRanges = KeyedStream.of(transactionsTableInteractions)
                    .mapKeys(TransactionsTableInteraction::getTransactionsTableName)
                    .map(TransactionsTableInteraction::getTimestampRange)
                    .collectToMap();
            Map<String, Integer> numPartitionKeysByTable =
                    KeyedStream.stream(partitionKeysByTable).map(Set::size).collectToMap();
            log.debug(
                    "Identified token ranges requiring repair in the following transactions tables",
                    SafeArg.of("transactionsTablesWithRanges", loggableTableRanges),
                    SafeArg.of("numPartitionKeysByTable", numPartitionKeysByTable));
        }

        // 4. get repair ranges
        // TODO(gs): don't recreate a session here, we already got this information before
        try (Session session = newSession(namespace)) {
            String keyspaceName = session.getLoggedKeyspace();
            Metadata metadata = session.getCluster().getMetadata();

            Set<InetSocketAddress> hosts = new HashSet<>(); // TODO(gs): move to somewhere where we have the hosts!!
            return KeyedStream.stream(partitionKeysByTable)
                    .map(ranges -> ClusterMetadataUtils.getTokenMapping(hosts, metadata, keyspaceName, ranges))
                    .map(this::makeLightweight)
                    .collectToMap();
        }
    }

    private Map<String, Set<Token>> getPartitionKeysByTable(
            Namespace namespace, List<TransactionsTableInteraction> transactionsTableInteractions) {
        try (Session session = newSession(namespace)) {
            String keyspaceName = session.getLoggedKeyspace();
            Metadata metadata = session.getCluster().getMetadata();
            return KeyedStream.of(transactionsTableInteractions.stream())
                    .mapKeys(TransactionsTableInteraction::getTransactionsTableName)
                    .map(interaction -> interaction.getPartitionTokens(
                            ClusterMetadataUtils.getTableMetadata(
                                    metadata, keyspaceName, interaction.getTransactionsTableName()),
                            session))
                    .collectToMap();
        }
    }

    // TODO(gs): think about design here - this is hacky
    private Session newSession(Namespace namespace) {
        return getCqlCluster(namespace).newSession();
    }

    // TODO(gs) memoise/cache
    private CqlCluster getCqlCluster(Namespace namespace) {
        CassandraKeyValueServiceConfig config = keyValueServiceConfigFactory.apply(namespace);
        return CqlCluster.create(config);
    }

    // VisibleForTesting
    public RangesForRepair getRangesToRepair(CqlCluster cqlCluster, Namespace namespace, String tableName) {
        Map<InetSocketAddress, Set<TokenRange>> tokenRanges = getTokenRangesToRepair(cqlCluster, namespace, tableName);
        return makeLightweight(tokenRanges);
    }

    private Map<InetSocketAddress, Set<TokenRange>> getTokenRangesToRepair(
            CqlCluster cqlCluster, Namespace namespace, String tableName) {
        String cassandraTableName = getCassandraTableName(namespace, tableName);
        return cqlCluster.getTokenRanges(cassandraTableName);
    }

    private String getCassandraTableName(Namespace namespace, String tableName) {
        TableReference tableRef = TableReference.create(toKvNamespace(namespace), tableName);
        return AbstractKeyValueService.internalTableName(tableRef);
    }

    private com.palantir.atlasdb.keyvalue.api.Namespace toKvNamespace(Namespace namespace) {
        return com.palantir.atlasdb.keyvalue.api.Namespace.create(namespace.get());
    }

    // TODO(gs): ugh. this gives us a stream in a stream in a stream
    private RangesForRepair makeLightweight(Map<InetSocketAddress, Set<TokenRange>> ranges) {
        return (RangesForRepair)
                KeyedStream.stream(ranges).map(this::makeLightweight).collectToMap();
    }

    private Set<Range<LightweightOppToken>> makeLightweight(Set<TokenRange> tokenRanges) {
        return tokenRanges.stream().flatMap(this::makeLightweight).collect(Collectors.toSet());
    }

    private Stream<Range<LightweightOppToken>> makeLightweight(TokenRange tokenRange) {
        LightweightOppToken startToken = LightweightOppToken.serialize(tokenRange.getStart());
        LightweightOppToken endToken = LightweightOppToken.serialize(tokenRange.getEnd());

        if (startToken.compareTo(endToken) <= 0) {
            return Stream.of(Range.closed(startToken, endToken));
        } else {
            // Handle wrap-around
            LightweightOppToken unbounded = unboundedToken();
            Range<LightweightOppToken> greaterThan = Range.closed(startToken, unbounded);
            Range<LightweightOppToken> atMost = Range.closed(unbounded, endToken);
            return Stream.of(greaterThan, atMost);
        }
    }

    private LightweightOppToken unboundedToken() {
        ByteBuffer minValue = ByteBuffer.allocate(0);
        return new LightweightOppToken(
                BaseEncoding.base16().decode(Bytes.toHexString(minValue).toUpperCase()));
    }
}
