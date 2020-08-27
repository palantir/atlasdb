/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.TimestampRangeDelete;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.Throwables;
import com.palantir.common.exception.AtlasDbDependencyException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraTableMetadata {
    private static final Logger log = LoggerFactory.getLogger(CassandraTableMetadata.class);
    private final RangeLoader rangeLoader;
    private final CassandraTables cassandraTables;
    private final CassandraClientPool clientPool;
    private final WrappingQueryRunner wrappingQueryRunner;

    public CassandraTableMetadata(RangeLoader rangeLoader, CassandraTables cassandraTables,
            CassandraClientPool clientPool, WrappingQueryRunner wrappingQueryRunner) {
        this.rangeLoader = rangeLoader;
        this.cassandraTables = cassandraTables;
        this.clientPool = clientPool;
        this.wrappingQueryRunner = wrappingQueryRunner;
    }

    public Map<TableReference, byte[]> getMetadataForTables() {
        Map<TableReference, Value> tableToMetadataContents;
        Map<TableReference, byte[]> result = Maps.newHashMap();

        Set<TableReference> allTableRefs = cassandraTables.getTableReferencesWithoutFiltering()
                .collect(Collectors.toSet());

        // we don't even have a metadata table yet. Return empty map.
        if (!allTableRefs.contains(AtlasDbConstants.DEFAULT_METADATA_TABLE)) {
            log.trace("getMetadata called with no _metadata table present");
            return ImmutableMap.of();
        }

        try (ClosableIterator<RowResult<Value>> range = rangeLoader.getRange(
                AtlasDbConstants.DEFAULT_METADATA_TABLE,
                CassandraKeyValueServices.metadataRangeRequest(),
                Long.MAX_VALUE)) {
            tableToMetadataContents = range.stream()
                    .map(RowResult::getCells)
                    .map(Iterables::getOnlyElement)
                    .collect(Collectors.toMap(
                            entry -> CassandraKeyValueServices
                                    .lowerCaseTableReferenceFromBytes(entry.getKey().getRowName()),
                            Map.Entry::getValue,
                            // take the lexicographically latest, which will be the new style entry, if it exists
                            (fst, snd) -> snd));
        }

        for (TableReference tableRef : allTableRefs) {
            if (HiddenTables.isHidden(tableRef)) {
                continue;
            }
            TableReference lowercaseTableRef = TableReference.createLowerCased(tableRef);
            if (tableToMetadataContents.containsKey(lowercaseTableRef)) {
                result.put(tableRef, tableToMetadataContents.get(lowercaseTableRef).getContents());
            }
        }

        return result;
    }

    void deleteAllMetadataRowsForTable(TableReference tableRef) {
        try (ClosableIterator<RowResult<Value>> range = rangeLoader.getRange(
                AtlasDbConstants.DEFAULT_METADATA_TABLE,
                CassandraKeyValueServices.metadataRangeRequestForTable(tableRef),
                Long.MAX_VALUE)) {
            Map<Cell, TimestampRangeDelete> cellsToDelete = range.stream()
                    .map(RowResult::getCells)
                    .map(Iterables::getOnlyElement)
                    .map(Map.Entry::getKey)
                    .map(Cell::getRowName)
                    .map(CassandraKeyValueServices::tableReferenceFromBytes)
                    .filter(candidate -> nonNullMatchingIgnoreCase(candidate, tableRef))
                    .collect(Collectors.toMap(CassandraKeyValueServices::getOldMetadataCell,
                            ignore -> new TimestampRangeDelete.Builder()
                                    .timestamp(Long.MAX_VALUE)
                                    .endInclusive(false) // true won't work, since we are deleting at Long.MAX_VALUE.
                                    .deleteSentinels(true)
                                    .build()));

            new CellRangeDeleter(clientPool, wrappingQueryRunner, CassandraKeyValueServiceImpl.DELETE_CONSISTENCY,
                    no -> System.currentTimeMillis())
                    .deleteAllTimestamps(AtlasDbConstants.DEFAULT_METADATA_TABLE, cellsToDelete);
        } catch (AtlasDbDependencyException e) {
            log.info("Failed to delete old table metadata for table {} because not all Cassandra nodes are up.",
                    LoggingArgs.tableRef(tableRef), e);
        }
    }

    Map<TableReference, byte[]> filterOutExistingTables(
            final Map<TableReference, byte[]> tableNamesToTableMetadata) {
        Map<TableReference, byte[]> filteredTables = Maps.newHashMap();
        try {
            Set<TableReference> existingTablesLowerCased = cassandraTables.getExistingLowerCased().stream()
                    .map(TableReference::fromInternalTableName)
                    .collect(Collectors.toSet());

            for (Map.Entry<TableReference, byte[]> tableAndMetadataPair : tableNamesToTableMetadata.entrySet()) {
                TableReference table = tableAndMetadataPair.getKey();
                byte[] metadata = tableAndMetadataPair.getValue();

                CassandraVerifier.sanityCheckTableName(table);

                TableReference tableRefLowerCased = TableReference.createLowerCased(table);
                if (!existingTablesLowerCased.contains(tableRefLowerCased)) {
                    filteredTables.put(table, metadata);
                } else {
                    log.debug("Filtering out existing table ({}) that already existed (case insensitive).",
                            LoggingArgs.tableRef(table));
                }
            }
        } catch (Exception e) {
            throw Throwables.unwrapAndThrowAtlasDbDependencyException(e);
        }

        return filteredTables;
    }

    Map<TableReference, byte[]> filterOutNoOpMetadataChanges(
            final Map<TableReference, byte[]> tableNamesToTableMetadata) {
        Map<TableReference, byte[]> existingTableMetadata = getMetadataForTables();
        Map<TableReference, byte[]> tableMetadataUpdates = Maps.newHashMap();

        for (Map.Entry<TableReference, byte[]> entry : tableNamesToTableMetadata.entrySet()) {
            TableReference tableReference = entry.getKey();
            byte[] newMetadata = entry.getValue();

            if (metadataIsDifferent(existingTableMetadata.get(tableReference), newMetadata)) {
                Set<TableReference> matchingTables = Sets.filter(existingTableMetadata.keySet(), existingTableRef ->
                        existingTableRef.getQualifiedName().equalsIgnoreCase(tableReference.getQualifiedName()));

                if (newTableOrUpdate(existingTableMetadata, newMetadata, matchingTables)) {
                    tableMetadataUpdates.put(tableReference, newMetadata);
                } else {
                    log.debug("Case-insensitive matched table already existed with same metadata,"
                            + " skipping update to {}", LoggingArgs.tableRef(tableReference));
                }
            } else {
                log.debug("Table already existed with same metadata, skipping update to {}",
                        LoggingArgs.tableRef(tableReference));
            }
        }

        return tableMetadataUpdates;
    }

    private static boolean metadataIsDifferent(byte[] existingMetadata, byte[] requestMetadata) {
        return !Arrays.equals(existingMetadata, requestMetadata);
    }

    private static boolean newTableOrUpdate(Map<TableReference, byte[]> existingMetadata, byte[] newMetadata,
            Set<TableReference> matchingTables) {
        return matchingTables.isEmpty()
                || metadataIsDifferent(existingMetadata.get(Iterables.getOnlyElement(matchingTables)), newMetadata);
    }

    private static boolean nonNullMatchingIgnoreCase(TableReference t1, TableReference t2) {
        return t1 != null && t2 != null && t1.getQualifiedName().equalsIgnoreCase(t2.getQualifiedName().toLowerCase());
    }
}
