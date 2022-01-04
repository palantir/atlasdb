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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.RangeSet;
import com.google.common.collect.Sets;
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
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Stream;

public class CassandraRepairHelper {
    private static final SafeLogger log = SafeLoggerFactory.get(CassandraRepairHelper.class);

    private static final Set<TableReference> TABLES_TO_REPAIR =
            Sets.union(ImmutableSet.of(AtlasDbConstants.COORDINATION_TABLE), TargetedSweepTables.REPAIR_ON_RESTORE);

    private final Function<Namespace, CassandraKeyValueServiceConfig> keyValueServiceConfigFactory;
    private final Function<Namespace, KeyValueService> keyValueServiceFactory;
    private final LoadingCache<Namespace, CqlCluster> cqlClusters;

    public CassandraRepairHelper(
            Function<Namespace, CassandraKeyValueServiceConfig> keyValueServiceConfigFactory,
            Function<Namespace, KeyValueService> keyValueServiceFactory) {
        this.keyValueServiceConfigFactory = keyValueServiceConfigFactory;
        this.keyValueServiceFactory = keyValueServiceFactory;

        this.cqlClusters = Caffeine.newBuilder()
                .maximumSize(100)
                .expireAfterAccess(Duration.ofMinutes(20L))
                .removalListener(CassandraRepairHelper::onRemoval)
                .build(this::getCqlClusterUncached);
    }

    private static void onRemoval(Namespace namespace, CqlCluster cqlCluster, RemovalCause _removalCause) {
        try {
            log.info("Closing cql cluster", SafeArg.of("namespace", namespace));
            cqlCluster.close();
        } catch (IOException ex) {
            log.warn("Failed to close CqlCluster", ex);
        }
    }

    private CqlCluster getCqlClusterUncached(Namespace namespace) {
        CassandraKeyValueServiceConfig config = keyValueServiceConfigFactory.apply(namespace);
        return CqlCluster.create(config);
    }

    public void repairInternalTables(Namespace namespace, BiConsumer<String, RangesForRepair> repairTable) {
        KeyValueService kvs = keyValueServiceFactory.apply(namespace);
        CqlCluster cqlCluster = cqlClusters.get(namespace);
        KeyedStream.of(getTableNamesToRepair(kvs))
                .map(tableName -> getRangesToRepair(cqlCluster, namespace, tableName))
                // TODO(gs): this will do repairs serially, instead of batched. Is this fine? Port batching from
                //   internal product?
                .forEach(repairTable);
    }

    private static Stream<String> getTableNamesToRepair(KeyValueService kvs) {
        return kvs.getAllTableNames().stream()
                .filter(TABLES_TO_REPAIR::contains)
                .map(TableReference::getTableName);
    }

    public void repairTransactionsTables(
            Namespace namespace,
            List<TransactionsTableInteraction> transactionsTableInteractions,
            BiConsumer<String, RangesForRepair> repairTable) {
        Map<String, RangesForRepair> tokenRangesForRepair =
                getRangesForRepairByTable(namespace, transactionsTableInteractions);

        tokenRangesForRepair.forEach((table, ranges) -> {
            log.info("Repairing ranges for table", SafeArg.of("table", table));
            repairTable.accept(table, ranges);
        });
    }

    private Map<String, RangesForRepair> getRangesForRepairByTable(
            Namespace namespace, List<TransactionsTableInteraction> transactionsTableInteractions) {
        return KeyedStream.stream(getRawRangesForRepairByTable(namespace, transactionsTableInteractions))
                .map(RangesForRepair::of)
                .collectToMap();
    }

    private Map<String, Map<InetSocketAddress, RangeSet<LightweightOppToken>>> getRawRangesForRepairByTable(
            Namespace namespace, List<TransactionsTableInteraction> transactionsTableInteractions) {
        return cqlClusters.get(namespace).getTransactionsTableRangesForRepair(transactionsTableInteractions);
    }

    // VisibleForTesting
    public static RangesForRepair getRangesToRepair(CqlCluster cqlCluster, Namespace namespace, String tableName) {
        Map<InetSocketAddress, RangeSet<LightweightOppToken>> tokenRanges =
                getTokenRangesToRepair(cqlCluster, namespace, tableName);
        return RangesForRepair.of(tokenRanges);
    }

    private static Map<InetSocketAddress, RangeSet<LightweightOppToken>> getTokenRangesToRepair(
            CqlCluster cqlCluster, Namespace namespace, String tableName) {
        String cassandraTableName = getCassandraTableName(namespace, tableName);
        return cqlCluster.getTokenRanges(cassandraTableName);
    }

    private static String getCassandraTableName(Namespace namespace, String tableName) {
        TableReference tableRef = TableReference.create(toKvNamespace(namespace), tableName);
        return AbstractKeyValueService.internalTableName(tableRef);
    }

    private static com.palantir.atlasdb.keyvalue.api.Namespace toKvNamespace(Namespace namespace) {
        return com.palantir.atlasdb.keyvalue.api.Namespace.create(namespace.get());
    }
}
