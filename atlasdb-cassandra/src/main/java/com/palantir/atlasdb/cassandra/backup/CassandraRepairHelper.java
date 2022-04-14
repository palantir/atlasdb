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
import com.palantir.atlasdb.backup.KvsRunner;
import com.palantir.atlasdb.backup.api.AtlasService;
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

    private final Function<AtlasService, CassandraKeyValueServiceConfig> keyValueServiceConfigFactory;
    private final LoadingCache<AtlasService, CqlCluster> cqlClusters;
    private final KvsRunner kvsRunner;

    public CassandraRepairHelper(
            KvsRunner kvsRunner, Function<AtlasService, CassandraKeyValueServiceConfig> keyValueServiceConfigFactory) {
        this.kvsRunner = kvsRunner;
        this.keyValueServiceConfigFactory = keyValueServiceConfigFactory;

        this.cqlClusters = Caffeine.newBuilder()
                .maximumSize(100)
                .expireAfterAccess(Duration.ofMinutes(20L))
                .removalListener(CassandraRepairHelper::onRemoval)
                .build(this::getCqlClusterUncached);
    }

    private static void onRemoval(AtlasService atlasService, CqlCluster cqlCluster, RemovalCause _removalCause) {
        try {
            log.info("Closing cql cluster", SafeArg.of("atlasService", atlasService));
            cqlCluster.close();
        } catch (IOException ex) {
            log.warn("Failed to close CqlCluster", ex);
        }
    }

    private CqlCluster getCqlClusterUncached(AtlasService atlasService) {
        CassandraKeyValueServiceConfig config = keyValueServiceConfigFactory.apply(atlasService);
        return CqlCluster.create(config);
    }

    public void repairInternalTables(AtlasService atlasService, BiConsumer<String, RangesForRepair> repairTable) {
        kvsRunner.run(atlasService, kvs -> repairInternalTables(kvs, atlasService, repairTable));
    }

    public Void repairInternalTables(
            KeyValueService kvs, AtlasService atlasService, BiConsumer<String, RangesForRepair> repairTable) {
        CqlCluster cqlCluster = cqlClusters.get(atlasService);
        KeyedStream.of(getTableNamesToRepair(kvs))
                .map(tableName -> getRangesToRepair(cqlCluster, atlasService, tableName))
                .forEach(repairTable);
        return null;
    }

    private static Stream<String> getTableNamesToRepair(KeyValueService kvs) {
        return kvs.getAllTableNames().stream()
                .filter(TABLES_TO_REPAIR::contains)
                .map(TableReference::getTableName);
    }

    public void repairTransactionsTables(
            AtlasService atlasService,
            List<TransactionsTableInteraction> transactionsTableInteractions,
            BiConsumer<String, RangesForRepair> repairTable) {
        Map<String, RangesForRepair> tokenRangesForRepair =
                getRangesForRepairByTable(atlasService, transactionsTableInteractions);

        tokenRangesForRepair.forEach((table, ranges) -> {
            log.info("Repairing ranges for table", SafeArg.of("table", table));
            repairTable.accept(table, ranges);
        });
    }

    public void cleanTransactionsTables(
            AtlasService atlasService,
            long startTimestamp,
            List<TransactionsTableInteraction> transactionsTableInteractions) {
        cqlClusters.get(atlasService).abortTransactions(startTimestamp, transactionsTableInteractions);
    }

    private Map<String, RangesForRepair> getRangesForRepairByTable(
            AtlasService atlasService, List<TransactionsTableInteraction> transactionsTableInteractions) {
        return KeyedStream.stream(getRawRangesForRepairByTable(atlasService, transactionsTableInteractions))
                .map(RangesForRepair::of)
                .collectToMap();
    }

    private Map<String, Map<InetSocketAddress, RangeSet<LightweightOppToken>>> getRawRangesForRepairByTable(
            AtlasService atlasService, List<TransactionsTableInteraction> transactionsTableInteractions) {
        return cqlClusters.get(atlasService).getTransactionsTableRangesForRepair(transactionsTableInteractions);
    }

    // VisibleForTesting
    public static RangesForRepair getRangesToRepair(
            CqlCluster cqlCluster, AtlasService atlasService, String tableName) {
        Map<InetSocketAddress, RangeSet<LightweightOppToken>> tokenRanges =
                getTokenRangesToRepair(cqlCluster, atlasService, tableName);
        return RangesForRepair.of(tokenRanges);
    }

    private static Map<InetSocketAddress, RangeSet<LightweightOppToken>> getTokenRangesToRepair(
            CqlCluster cqlCluster, AtlasService atlasService, String tableName) {
        String cassandraTableName = getCassandraTableName(atlasService.getNamespace(), tableName);
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
