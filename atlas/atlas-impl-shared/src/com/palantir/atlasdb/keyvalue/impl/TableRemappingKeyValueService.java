// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.keyvalue.impl;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.ForwardingObject;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.NamespacedKeyValueService;
import com.palantir.atlasdb.keyvalue.TableMappingService;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.schema.Namespace;
import com.palantir.atlasdb.schema.TableReference;
import com.palantir.common.base.ClosableIterator;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

public class TableRemappingKeyValueService extends ForwardingObject implements
        NamespacedKeyValueService {
    public static TableRemappingKeyValueService create(KeyValueService delegate,
                                                       TableMappingService tableMapper) {
        return new TableRemappingKeyValueService(delegate, tableMapper);
    }

    private final KeyValueService delegate;

    private final TableMappingService tableMapper;

    private TableRemappingKeyValueService(KeyValueService delegate, TableMappingService tableMapper) {
        this.delegate = delegate;
        this.tableMapper = tableMapper;
    }

    @Override
    public void addGarbageCollectionSentinelValues(TableReference tableRef, Set<Cell> cells) {
        delegate().addGarbageCollectionSentinelValues(
                tableMapper.getShortTableName(tableRef),
                cells);
    }

    @Override
    public void createTable(TableReference tableRef, int maxValueSize) {
        String shortName = tableMapper.addTable(tableRef);
        delegate().createTable(shortName, maxValueSize);
    }

    @Override
    public void createTables(Map<TableReference, Integer> tableReferencesToMaxValueSizeInBytes) {
        Map<String, Integer> tableNameToMaxValueSize = Maps.newHashMapWithExpectedSize(tableReferencesToMaxValueSizeInBytes.size());
        for (TableReference table : tableReferencesToMaxValueSizeInBytes.keySet()) {
            tableNameToMaxValueSize.put(
                    tableMapper.addTable(table),
                    tableReferencesToMaxValueSizeInBytes.get(table));
        }
        delegate().createTables(tableNameToMaxValueSize);
    }

    @Override
    protected KeyValueService delegate() {
        return delegate;
    }

    @Override
    public void delete(TableReference tableRef, Multimap<Cell, Long> keys) {
        delegate().delete(tableMapper.getShortTableName(tableRef), keys);
    }

    @Override
    public void dropTable(TableReference tableRef) {
        delegate().dropTable(tableMapper.getShortTableName(tableRef));
        // Handles the edge case of deleting _namespace when clearing the kvs
        if (tableRef.getNamespace().equals(Namespace.EMPTY_NAMESPACE)
                && tableRef.getTablename().equals(AtlasDbConstants.NAMESPACE_TABLE)) {
            return;
        }
        tableMapper.removeTable(tableRef);
    }

    @Override
    public Map<Cell, Value> get(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        return delegate().get(tableMapper.getShortTableName(tableRef), timestampByCell);
    }

    @Override
    public Set<TableReference> getAllTableNames() {
        return tableMapper.mapToFullTableNames(delegate().getAllTableNames());
    }

    @Override
    public Multimap<Cell, Long> getAllTimestamps(TableReference tableRef,
                                                 Set<Cell> keys,
                                                 long timestamp) {
        return delegate().getAllTimestamps(tableMapper.getShortTableName(tableRef), keys, timestamp);
    }

    @Override
    public Collection<? extends KeyValueService> getDelegates() {
        return ImmutableSet.of(delegate);
    }

    @Override
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(TableReference tableRef,
                                                                                                           Iterable<RangeRequest> rangeRequests,
                                                                                                           long timestamp) {
        return delegate().getFirstBatchForRanges(
                tableMapper.getShortTableName(tableRef),
                rangeRequests,
                timestamp);
    }

    @Override
    public Map<Cell, Long> getLatestTimestamps(TableReference tableRef,
                                               Map<Cell, Long> timestampByCell) {
        return delegate().getLatestTimestamps(
                tableMapper.getShortTableName(tableRef),
                timestampByCell);
    }

    @Override
    public byte[] getMetadataForTable(TableReference tableRef) {
        return delegate().getMetadataForTable(tableMapper.getShortTableName(tableRef));
    }

    @Override
    public Map<TableReference, byte[]> getMetadataForTables() {
        Map<TableReference, byte[]> tableRefToBytes = Maps.newHashMap();
        for (Entry<String, byte[]> entry : delegate().getMetadataForTables().entrySet()) {
            tableRefToBytes.put(
                    Iterables.getOnlyElement(tableMapper.mapToFullTableNames(ImmutableSet.of(entry.getKey()))),
                    entry.getValue());
        }
        return tableRefToBytes;
    }

    @Override
    public ClosableIterator<RowResult<Value>> getRange(TableReference tableRef,
                                                       RangeRequest rangeRequest,
                                                       long timestamp) {
        return delegate().getRange(tableMapper.getShortTableName(tableRef), rangeRequest, timestamp);
    }

    @Override
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(TableReference tableRef,
                                                                       RangeRequest rangeRequest,
                                                                       long timestamp) {
        return delegate().getRangeOfTimestamps(
                tableMapper.getShortTableName(tableRef),
                rangeRequest,
                timestamp);
    }

    @Override
    public ClosableIterator<RowResult<Set<Value>>> getRangeWithHistory(TableReference tableReference,
                                                                       RangeRequest rangeRequest,
                                                                       long timestamp) {
        return delegate().getRangeWithHistory(
                tableMapper.getShortTableName(tableReference),
                rangeRequest,
                timestamp);
    }

    @Override
    public Map<Cell, Value> getRows(TableReference tableRef,
                                    Iterable<byte[]> rows,
                                    ColumnSelection columnSelection,
                                    long timestamp) {
        return delegate().getRows(
                tableMapper.getShortTableName(tableRef),
                rows,
                columnSelection,
                timestamp);
    }

    @Override
    public void initializeFromFreshInstance() {
        delegate.initializeFromFreshInstance();
    }

    @Override
    public void multiPut(Map<TableReference, ? extends Map<Cell, byte[]>> valuesByTable,
                         long timestamp) {
        delegate().multiPut(tableMapper.mapToShortTableNames(valuesByTable), timestamp);
    }

    @Override
    public void put(TableReference tableRef, Map<Cell, byte[]> values, long timestamp) {
        delegate().put(tableMapper.getShortTableName(tableRef), values, timestamp);
    }

    @Override
    public void putMetadataForTable(TableReference tableRef, byte[] metadata) {
        delegate().putMetadataForTable(tableMapper.getShortTableName(tableRef), metadata);
    }

    @Override
    public void putMetadataForTables(Map<TableReference, byte[]> tableReferencesToMetadata) {
        Map<String, byte[]> tableNameToMetadata = Maps.newHashMapWithExpectedSize(tableReferencesToMetadata.size());
        for (TableReference tableRef : tableReferencesToMetadata.keySet()) {
            tableNameToMetadata.put(
                    tableMapper.getShortTableName(tableRef),
                    tableReferencesToMetadata.get(tableRef));
        }
        delegate().putMetadataForTables(tableNameToMetadata);
    }

    @Override
    public void putUnlessExists(TableReference tableRef, Map<Cell, byte[]> values)
            throws KeyAlreadyExistsException {
        delegate().putUnlessExists(tableMapper.getShortTableName(tableRef), values);
    }

    @Override
    public void putWithTimestamps(TableReference tableRef, Multimap<Cell, Value> values) {
        delegate().putWithTimestamps(tableMapper.getShortTableName(tableRef), values);
    }

    @Override
    public void close() {
        delegate().close();
    }

    @Override
    public void teardown() {
        delegate().teardown();
    }

    @Override
    public void truncateTable(TableReference tableRef) {
        delegate().truncateTable(tableMapper.getShortTableName(tableRef));
    }

    @Override
    public void truncateTables(Set<TableReference> tableRefs) {
        Set<String> tablesToTruncate = Sets.newHashSet();
        for (TableReference tableRef : tableRefs) {
            tablesToTruncate.add(tableMapper.getShortTableName(tableRef));
        }
        delegate().truncateTables(tablesToTruncate);
    }

    @Override
    public void compactInternally(TableReference tableRef) {
        delegate().compactInternally(tableMapper.getShortTableName(tableRef));
    }
}
