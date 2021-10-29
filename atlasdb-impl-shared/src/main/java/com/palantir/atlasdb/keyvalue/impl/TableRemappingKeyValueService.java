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
package com.palantir.atlasdb.keyvalue.impl;

import com.google.common.collect.ForwardingObject;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.TableMappingService;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetCompatibility;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.ClusterAvailabilityStatus;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.TimestampRangeDelete;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.exception.TableMappingNotFoundException;
import com.palantir.util.paging.TokenBackedBasicResultsPage;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public final class TableRemappingKeyValueService extends ForwardingObject implements KeyValueService {
    public static TableRemappingKeyValueService create(KeyValueService delegate, TableMappingService tableMapper) {
        return new TableRemappingKeyValueService(delegate, tableMapper);
    }

    private final KeyValueService delegate;

    private final TableMappingService tableMapper;

    private TableRemappingKeyValueService(KeyValueService delegate, TableMappingService tableMapper) {
        this.delegate = delegate;
        this.tableMapper = tableMapper;
    }

    @Override
    public void addGarbageCollectionSentinelValues(TableReference tableRef, Iterable<Cell> cells) {
        try {
            delegate().addGarbageCollectionSentinelValues(tableMapper.getMappedTableName(tableRef), cells);
        } catch (TableMappingNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void createTable(TableReference tableRef, byte[] tableMetadata) {
        TableReference shortName = tableMapper.addTable(tableRef);
        delegate().createTable(shortName, tableMetadata);
    }

    @Override
    public void createTables(Map<TableReference, byte[]> tableReferencesToTableMetadata) {
        Map<TableReference, byte[]> tableNameToTableMetadata =
                Maps.newHashMapWithExpectedSize(tableReferencesToTableMetadata.size());
        for (Map.Entry<TableReference, byte[]> tableEntry : tableReferencesToTableMetadata.entrySet()) {
            tableNameToTableMetadata.put(tableMapper.addTable(tableEntry.getKey()), tableEntry.getValue());
        }
        delegate().createTables(tableNameToTableMetadata);
    }

    @Override
    protected KeyValueService delegate() {
        return delegate;
    }

    @Override
    public void delete(TableReference tableRef, Multimap<Cell, Long> keys) {
        try {
            delegate().delete(tableMapper.getMappedTableName(tableRef), keys);
        } catch (TableMappingNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void deleteRange(TableReference tableRef, RangeRequest range) {
        try {
            delegate().deleteRange(tableMapper.getMappedTableName(tableRef), range);
        } catch (TableMappingNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void deleteRows(TableReference tableRef, Iterable<byte[]> rows) {
        try {
            delegate().deleteRows(tableMapper.getMappedTableName(tableRef), rows);
        } catch (TableMappingNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void deleteAllTimestamps(TableReference tableRef, Map<Cell, TimestampRangeDelete> deletes) {
        try {
            delegate().deleteAllTimestamps(tableMapper.getMappedTableName(tableRef), deletes);
        } catch (TableMappingNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void dropTable(TableReference tableRef) {
        dropTables(ImmutableSet.of(tableRef));
    }

    @Override
    public void dropTables(Set<TableReference> tableRefs) {
        Set<TableReference> tableNames = getShortTableReferencesForExistingTables(tableRefs);
        delegate().dropTables(tableNames);

        // We're purposely updating the table mappings after all drops are complete
        tableMapper.removeTables(tableRefs.stream()
                .filter(tableRef -> !tableRef.equals(AtlasDbConstants.NAMESPACE_TABLE))
                .collect(Collectors.toSet()));
    }

    private Set<TableReference> getShortTableReferencesForExistingTables(Set<TableReference> tableRefs) {
        Set<TableReference> tableNames = Sets.newHashSetWithExpectedSize(tableRefs.size());
        for (TableReference tableRef : tableRefs) {
            try {
                tableNames.add(tableMapper.getMappedTableName(tableRef));
            } catch (TableMappingNotFoundException e) {
                // Table does not exist - do nothing
            }
        }
        return tableNames;
    }

    @Override
    public Map<Cell, Value> get(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        try {
            return delegate().get(tableMapper.getMappedTableName(tableRef), timestampByCell);
        } catch (TableMappingNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public Set<TableReference> getAllTableNames() {
        return ImmutableSet.copyOf(tableMapper
                .generateMapToFullTableNames(delegate().getAllTableNames())
                .values());
    }

    @Override
    public Multimap<Cell, Long> getAllTimestamps(TableReference tableRef, Set<Cell> keys, long timestamp) {
        try {
            return delegate().getAllTimestamps(tableMapper.getMappedTableName(tableRef), keys, timestamp);
        } catch (TableMappingNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public Collection<? extends KeyValueService> getDelegates() {
        return ImmutableSet.of(delegate);
    }

    @Override
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(
            TableReference tableRef, Iterable<RangeRequest> rangeRequests, long timestamp) {
        try {
            return delegate()
                    .getFirstBatchForRanges(tableMapper.getMappedTableName(tableRef), rangeRequests, timestamp);
        } catch (TableMappingNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public Map<Cell, Long> getLatestTimestamps(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        try {
            return delegate().getLatestTimestamps(tableMapper.getMappedTableName(tableRef), timestampByCell);
        } catch (TableMappingNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public byte[] getMetadataForTable(TableReference tableRef) {
        try {
            return delegate().getMetadataForTable(tableMapper.getMappedTableName(tableRef));
        } catch (TableMappingNotFoundException e) { // table does not exist.
            return AtlasDbConstants.EMPTY_TABLE_METADATA;
        }
    }

    @Override
    public Map<TableReference, byte[]> getMetadataForTables() {
        Map<TableReference, byte[]> tableMetadata = delegate().getMetadataForTables();
        Map<TableReference, TableReference> metadataNamesToFullTableNames =
                tableMapper.generateMapToFullTableNames(tableMetadata.keySet());
        Map<TableReference, byte[]> fullTableNameToBytes =
                Maps.newHashMapWithExpectedSize(metadataNamesToFullTableNames.size());
        for (Map.Entry<TableReference, byte[]> entry : tableMetadata.entrySet()) {
            fullTableNameToBytes.put(metadataNamesToFullTableNames.get(entry.getKey()), entry.getValue());
        }
        return fullTableNameToBytes;
    }

    @Override
    public ClosableIterator<RowResult<Value>> getRange(
            TableReference tableRef, RangeRequest rangeRequest, long timestamp) {
        try {
            return delegate().getRange(tableMapper.getMappedTableName(tableRef), rangeRequest, timestamp);
        } catch (TableMappingNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(
            TableReference tableRef, RangeRequest rangeRequest, long timestamp) {
        try {
            return delegate().getRangeOfTimestamps(tableMapper.getMappedTableName(tableRef), rangeRequest, timestamp);
        } catch (TableMappingNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public ClosableIterator<List<CandidateCellForSweeping>> getCandidateCellsForSweeping(
            TableReference tableRef, CandidateCellForSweepingRequest request) {
        try {
            return delegate().getCandidateCellsForSweeping(tableMapper.getMappedTableName(tableRef), request);
        } catch (TableMappingNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public Map<Cell, Value> getRows(
            TableReference tableRef, Iterable<byte[]> rows, ColumnSelection columnSelection, long timestamp) {
        try {
            return delegate().getRows(tableMapper.getMappedTableName(tableRef), rows, columnSelection, timestamp);
        } catch (TableMappingNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public Map<byte[], RowColumnRangeIterator> getRowsColumnRange(
            TableReference tableRef,
            Iterable<byte[]> rows,
            BatchColumnRangeSelection columnRangeSelection,
            long timestamp) {
        try {
            return delegate()
                    .getRowsColumnRange(
                            tableMapper.getMappedTableName(tableRef), rows, columnRangeSelection, timestamp);
        } catch (TableMappingNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public RowColumnRangeIterator getRowsColumnRange(
            TableReference tableRef,
            Iterable<byte[]> rows,
            ColumnRangeSelection columnRangeSelection,
            int cellBatchHint,
            long timestamp) {
        try {
            return delegate()
                    .getRowsColumnRange(
                            tableMapper.getMappedTableName(tableRef),
                            rows,
                            columnRangeSelection,
                            cellBatchHint,
                            timestamp);
        } catch (TableMappingNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void multiPut(Map<TableReference, ? extends Map<Cell, byte[]>> valuesByTable, long timestamp) {
        try {
            delegate().multiPut(tableMapper.mapToShortTableNames(valuesByTable), timestamp);
        } catch (TableMappingNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void put(TableReference tableRef, Map<Cell, byte[]> values, long timestamp) {
        try {
            delegate().put(tableMapper.getMappedTableName(tableRef), values, timestamp);
        } catch (TableMappingNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void putMetadataForTable(TableReference tableRef, byte[] metadata) {
        try {
            delegate().putMetadataForTable(tableMapper.getMappedTableName(tableRef), metadata);
        } catch (TableMappingNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void putMetadataForTables(Map<TableReference, byte[]> tableReferencesToMetadata) {
        Map<TableReference, byte[]> tableNameToMetadata =
                Maps.newHashMapWithExpectedSize(tableReferencesToMetadata.size());
        for (Map.Entry<TableReference, byte[]> tableEntry : tableReferencesToMetadata.entrySet()) {
            try {
                tableNameToMetadata.put(tableMapper.getMappedTableName(tableEntry.getKey()), tableEntry.getValue());
            } catch (TableMappingNotFoundException e) {
                throw new IllegalArgumentException(e);
            }
        }
        delegate().putMetadataForTables(tableNameToMetadata);
    }

    @Override
    public void putUnlessExists(TableReference tableRef, Map<Cell, byte[]> values) throws KeyAlreadyExistsException {
        try {
            delegate().putUnlessExists(tableMapper.getMappedTableName(tableRef), values);
        } catch (TableMappingNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void putToCasTable(TableReference tableRef, Map<Cell, byte[]> values) {
        try {
            delegate().putToCasTable(tableMapper.getMappedTableName(tableRef), values);
        } catch (TableMappingNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public CheckAndSetCompatibility getCheckAndSetCompatibility() {
        return delegate().getCheckAndSetCompatibility();
    }

    @Override
    public void checkAndSet(CheckAndSetRequest checkAndSetRequest) {
        try {
            CheckAndSetRequest request = new CheckAndSetRequest.Builder()
                    .from(checkAndSetRequest)
                    .table(tableMapper.getMappedTableName(checkAndSetRequest.table()))
                    .build();
            delegate().checkAndSet(request);
        } catch (TableMappingNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void putWithTimestamps(TableReference tableRef, Multimap<Cell, Value> values) {
        try {
            delegate().putWithTimestamps(tableMapper.getMappedTableName(tableRef), values);
        } catch (TableMappingNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void close() {
        delegate().close();
    }

    @Override
    public void truncateTable(TableReference tableRef) {
        try {
            delegate().truncateTable(tableMapper.getMappedTableName(tableRef));
        } catch (TableMappingNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void truncateTables(Set<TableReference> tableRefs) {
        Set<TableReference> tablesToTruncate = new HashSet<>();
        for (TableReference tableRef : tableRefs) {
            try {
                tablesToTruncate.add(tableMapper.getMappedTableName(tableRef));
            } catch (TableMappingNotFoundException e) {
                throw new IllegalArgumentException(e);
            }
        }
        delegate().truncateTables(tablesToTruncate);
    }

    @Override
    public void compactInternally(TableReference tableRef) {
        try {
            delegate().compactInternally(tableMapper.getMappedTableName(tableRef));
        } catch (TableMappingNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void compactInternally(TableReference tableRef, boolean inMaintenanceMode) {
        try {
            delegate().compactInternally(tableMapper.getMappedTableName(tableRef), inMaintenanceMode);
        } catch (TableMappingNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public ClusterAvailabilityStatus getClusterAvailabilityStatus() {
        return delegate().getClusterAvailabilityStatus();
    }

    @Override
    public boolean shouldTriggerCompactions() {
        return delegate.shouldTriggerCompactions();
    }

    @Override
    public List<byte[]> getRowKeysInRange(TableReference tableRef, byte[] startRow, byte[] endRow, int maxResults) {
        try {
            return delegate().getRowKeysInRange(tableMapper.getMappedTableName(tableRef), startRow, endRow, maxResults);
        } catch (TableMappingNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public ListenableFuture<Map<Cell, Value>> getAsync(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        try {
            return delegate().getAsync(tableMapper.getMappedTableName(tableRef), timestampByCell);
        } catch (TableMappingNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
