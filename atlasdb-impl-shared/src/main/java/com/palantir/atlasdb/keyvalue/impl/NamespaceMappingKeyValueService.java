/**
 * Copyright 2015 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.impl;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.ForwardingObject;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.NamespacedKeyValueService;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.schema.TableReference;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.common.base.ClosableIterator;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

public class NamespaceMappingKeyValueService extends ForwardingObject implements KeyValueService {
    private final NamespacedKeyValueService delegate;

    public static NamespaceMappingKeyValueService create(NamespacedKeyValueService delegate) {
        return new NamespaceMappingKeyValueService(delegate);
    }

    NamespaceMappingKeyValueService(NamespacedKeyValueService delegate) {
        this.delegate = delegate;
    }

    protected TableReference getTableReference(String tableName) {
        return delegate().getTableMapper().getTableReference(tableName);
    }

    protected Set<String> resolveNamespacedSet(Set<TableReference> set) {
        Set<String> resolvedSet = Sets.newHashSet();
        for (TableReference tableRef : set) {
            resolvedSet.add(convertTableReferenceToString(tableRef));
        }
        return resolvedSet;
    }

    protected final String convertTableReferenceToString(TableReference tableRef) {
        return Schemas.getFullTableName(tableRef.getTablename(), tableRef.getNamespace());
    }

    @Override
    protected NamespacedKeyValueService delegate() {
        return delegate;
    }

    @Override
    public void createTable(String tableName, byte[] tableMetadata) {
        delegate().createTable(getTableReference(tableName), tableMetadata);
    }

    @Override
    public void delete(String tableName, Multimap<Cell, Long> keys) {
        delegate().delete(getTableReference(tableName), keys);
    }

    @Override
    public Multimap<Cell, Long> getAllTimestamps(String tableName, Set<Cell> keys, long timestamp) {
        return delegate().getAllTimestamps(getTableReference(tableName), keys, timestamp);
    }

    @Override
    public void dropTable(String tableName) {
        delegate().dropTable(getTableReference(tableName));
    }

    @Override
    public void dropTables(Set<String> tableNames) {
        Set<TableReference> tableReferences = Sets.newHashSet();
        for (String tableName : tableNames) {
            tableReferences.add(getTableReference(tableName));
        }
        delegate().dropTables(tableReferences);
    }

    @Override
    public Map<Cell, Value> get(String tableName, Map<Cell, Long> timestampByCell) {
        return delegate().get(getTableReference(tableName), timestampByCell);
    }

    @Override
    public Map<Cell, Value> getRows(String tableName, Iterable<byte[]> rows,
                                    ColumnSelection columnSelection, long timestamp) {
        return delegate().getRows(getTableReference(tableName), rows, columnSelection, timestamp);
    }

    @Override
    public Map<Cell, Long> getLatestTimestamps(String tableName, Map<Cell, Long> timestampByCell) {
        return delegate().getLatestTimestamps(getTableReference(tableName), timestampByCell);
    }

    @Override
    public ClosableIterator<RowResult<Value>> getRange(String tableName, RangeRequest rangeRequest, long timestamp) {
        return delegate().getRange(getTableReference(tableName), rangeRequest, timestamp);
    }

    @Override
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(String tableName, RangeRequest rangeRequest, long timestamp) {
        return delegate().getRangeOfTimestamps(getTableReference(tableName), rangeRequest, timestamp);
    }

    @Override
    public ClosableIterator<RowResult<Set<Value>>> getRangeWithHistory(String tableName,
                                                                RangeRequest rangeRequest,
                                                                long timestamp) {
        return delegate().getRangeWithHistory(getTableReference(tableName), rangeRequest, timestamp);
    }

    @Override
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(String tableName,
            Iterable<RangeRequest> rangeRequests,
            long timestamp) {
        return delegate().getFirstBatchForRanges(getTableReference(tableName), rangeRequests, timestamp);
    }

    @Override
    public void put(String tableName, Map<Cell, byte[]> values, long timestamp) {
        delegate().put(getTableReference(tableName), values, timestamp);
    }

    @Override
    public void putWithTimestamps(String tableName, Multimap<Cell, Value> cellValues) {
        delegate().putWithTimestamps(getTableReference(tableName), cellValues);
    }

    @Override
    public void multiPut(Map<String, ? extends Map<Cell, byte[]>> valuesByTable, long timestamp) {
        Map<TableReference, ? extends Map<Cell, byte[]>> map = getNamespacedMap(valuesByTable);
        delegate().multiPut(map, timestamp);
    }

    private <T> Map<TableReference, T> getNamespacedMap(Map<String, T> map) {
        Map<TableReference, T> namespacedMap = Maps.newHashMap();
        for (Entry<String, T> e : map.entrySet()) {
            namespacedMap.put(getTableReference(e.getKey()), e.getValue());
        }
        return namespacedMap;
    }

    @Override
    public void putUnlessExists(String tableName, Map<Cell, byte[]> values)
            throws KeyAlreadyExistsException {
        delegate().putUnlessExists(getTableReference(tableName), values);
    }

    @Override
    public void truncateTable(String tableName) {
        delegate().truncateTable(getTableReference(tableName));
    }

    @Override
    public void truncateTables(Set<String> tableNames) {
        Set<TableReference> tableRefs = Sets.newHashSet();
        for (String tableName : tableNames) {
            tableRefs.add(getTableReference(tableName));
        }
        delegate().truncateTables(tableRefs);
    }

    @Override
    public byte[] getMetadataForTable(String tableName) {
        return delegate().getMetadataForTable(getTableReference(tableName));
    }

    @Override
    public Map<String, byte[]> getMetadataForTables() {
        Map<TableReference, byte[]> tableRefMetadata = delegate().getMetadataForTables();
        Map<String, byte[]> tableNameMetadata = Maps.newHashMapWithExpectedSize(tableRefMetadata.size());
        for (Entry<TableReference, byte[]> entry : tableRefMetadata.entrySet()) {
            tableNameMetadata.put(convertTableReferenceToString(entry.getKey()), entry.getValue());
        }
        return tableNameMetadata;
    }

    @Override
    public void putMetadataForTable(String tableName, byte[] metadata) {
        delegate().putMetadataForTable(getTableReference(tableName), metadata);
    }

    @Override
    public Set<String> getAllTableNames() {
        return resolveNamespacedSet(delegate().getAllTableNames());
    }

    @Override
    public void addGarbageCollectionSentinelValues(String tableName, Set<Cell> cells) {
        delegate().addGarbageCollectionSentinelValues(getTableReference(tableName), cells);
    }

    @Override
    public void initializeFromFreshInstance() {
        delegate().initializeFromFreshInstance();
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
    public Collection<? extends KeyValueService> getDelegates() {
        return delegate().getDelegates();
    }

    @Override
    public void createTables(Map<String, byte[]> tableNameToTableMetadata) {
        Map<TableReference, byte[]> tableReferencesToTableMetadata = Maps.newHashMapWithExpectedSize(tableNameToTableMetadata.size());
        for (Entry<String, byte[]> tableToMetadata : tableNameToTableMetadata.entrySet()) {
            tableReferencesToTableMetadata.put(getTableReference(tableToMetadata.getKey()), tableToMetadata.getValue());
        }
        delegate().createTables(tableReferencesToTableMetadata);
    }

    @Override
    public void putMetadataForTables(Map<String, byte[]> tableNameToMetadata) {
        Map<TableReference, byte[]> tableReferencesToMetadata = Maps.newHashMapWithExpectedSize(tableNameToMetadata.size());
        for (Entry<String, byte[]> tableToMetadataEntry : tableNameToMetadata.entrySet()) {
            tableReferencesToMetadata.put(getTableReference(tableToMetadataEntry.getKey()), tableToMetadataEntry.getValue());
        }
        delegate().putMetadataForTables(tableReferencesToMetadata);
    }

    @Override
    public void compactInternally(String tableName) {
        delegate().compactInternally(getTableReference(tableName));
    }
}
