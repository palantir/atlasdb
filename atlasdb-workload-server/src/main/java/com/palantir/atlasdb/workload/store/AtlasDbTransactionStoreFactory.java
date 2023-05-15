/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.workload.store;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbRuntimeConfig;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.workload.util.AtlasDbUtils;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.lock.client.TimeLockClient;
import com.palantir.lock.client.UnreliableTimeLockService;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.refreshable.Refreshable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import one.util.streamex.EntryStream;
import one.util.streamex.StreamEx;

public final class AtlasDbTransactionStoreFactory implements TransactionStoreFactory<InteractiveTransactionStore> {

    // Purposefully override the lock refresh interval to increase our chances of losing locks.
    private static final int LOCK_REFRESH_INTERVAL_MS = 100;

    private final TransactionManager transactionManager;
    private final Optional<Namespace> maybeNamespace;

    public AtlasDbTransactionStoreFactory(TransactionManager transactionManager, Optional<String> maybeNamespace) {
        this.transactionManager = transactionManager;
        this.maybeNamespace = maybeNamespace.map(Namespace::create);
    }

    @Override
    public InteractiveTransactionStore create(Map<String, IsolationLevel> tables, Set<IndexTable> indexes) {
        return AtlasDbTransactionStore.create(transactionManager, toAtlasTables(tables, indexes));
    }

    @VisibleForTesting
    Map<TableReference, byte[]> toAtlasTables(Map<String, IsolationLevel> tables, Set<IndexTable> indexTables) {
        checkIndexesReferenceExistingPrimaryTable(tables, indexTables);
        checkIndexAndPrimaryTableNamesDoNotConflict(tables, indexTables);
        checkIndexesDoNotHaveDuplicateNames(indexTables);

        Map<TableReference, ConflictHandler> atlasPrimaryTables = EntryStream.of(tables)
                .mapKeys(this::createTableReference)
                .mapValues(AtlasDbUtils::toConflictHandler)
                .toMap();
        Map<TableReference, ConflictHandler> atlasIndexTables = StreamEx.of(indexTables)
                .mapToEntry(IndexTable::indexTableName, IndexTable::primaryTableName)
                .mapKeys(this::createTableReference)
                .mapValues(tables::get)
                .mapValues(AtlasDbUtils::toConflictHandler)
                .mapValues(AtlasDbUtils::toIndexConflictHandler)
                .toMap();
        return EntryStream.of(atlasPrimaryTables)
                .append(atlasIndexTables)
                .mapValues(AtlasDbUtils::tableMetadata)
                .toMap();
    }

    @VisibleForTesting
    TableReference createTableReference(String tableName) {
        return maybeNamespace
                .map(namespace -> TableReference.create(namespace, tableName))
                .orElseGet(() -> TableReference.createWithEmptyNamespace(tableName));
    }

    private static void checkIndexesDoNotHaveDuplicateNames(Set<IndexTable> indexTables) {
        Map<String, Long> countOfIndexNames = indexTables.stream()
                .map(IndexTable::indexTableName)
                .map(String::toLowerCase)
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        Set<String> duplicateTableIndexNames = EntryStream.of(countOfIndexNames)
                .removeValues(value -> value <= 1)
                .keys()
                .toSet();

        Preconditions.checkArgument(
                duplicateTableIndexNames.isEmpty(),
                "Found index tables with the same name",
                SafeArg.of("duplicateTableIndexNames", duplicateTableIndexNames));
    }

    private static void checkIndexAndPrimaryTableNamesDoNotConflict(
            Map<String, IsolationLevel> tables, Set<IndexTable> indexTables) {
        Set<String> primaryTableNames =
                tables.keySet().stream().map(String::toLowerCase).collect(Collectors.toSet());
        Set<String> indexTableNames = indexTables.stream()
                .map(IndexTable::indexTableName)
                .map(String::toLowerCase)
                .collect(Collectors.toSet());
        Set<String> conflictingTableNames = Sets.intersection(primaryTableNames, indexTableNames);
        Preconditions.checkArgument(
                conflictingTableNames.isEmpty(),
                "Found indexes which have the same name as primary tables",
                SafeArg.of("conflictingTableNames", conflictingTableNames));
    }

    private static void checkIndexesReferenceExistingPrimaryTable(
            Map<String, IsolationLevel> tables, Set<IndexTable> indexTables) {
        Set<IndexTable> indexesWithUnknownPrimaryTables = StreamEx.of(indexTables)
                .remove(indexTable -> tables.containsKey(indexTable.primaryTableName()))
                .toSet();

        Preconditions.checkArgument(
                indexesWithUnknownPrimaryTables.isEmpty(),
                "Found indexes which reference an unknown primary table",
                SafeArg.of("indexesWithUnknownPrimaryTables", indexesWithUnknownPrimaryTables));
    }

    public static AtlasDbTransactionStoreFactory createFromConfig(
            AtlasDbConfig installConfig,
            Refreshable<Optional<AtlasDbRuntimeConfig>> atlasDbRuntimeConfig,
            UserAgent userAgent,
            MetricsManager metricsManager) {
        TransactionManager transactionManager = TransactionManagers.builder()
                .config(installConfig)
                .userAgent(userAgent)
                .globalMetricsRegistry(metricsManager.getRegistry())
                .globalTaggedMetricRegistry(metricsManager.getTaggedRegistry())
                .runtimeConfigSupplier(atlasDbRuntimeConfig)
                .defaultTimelockClientFactory(lockService -> TimeLockClient.createDefault(
                        UnreliableTimeLockService.create(lockService), LOCK_REFRESH_INTERVAL_MS))
                .build()
                .serializable();

        return new AtlasDbTransactionStoreFactory(
                transactionManager, installConfig.namespace().or(installConfig.keyValueService()::namespace));
    }
}
