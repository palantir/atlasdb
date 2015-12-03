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
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.schema.TableReference;
import com.palantir.common.base.ClosableIterator;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

/*
 * WARNING: Code assumes that this will be called above namespace mapping layer.
 *          Code also assumes that the more specific mapping (by table)
 *          trumps the more generic mapping (by namespace).
 */
public class TableSplittingKeyValueService implements KeyValueService {
    public static TableSplittingKeyValueService create(List<KeyValueService> delegates,
                                                       Map<String, KeyValueService> delegateByTable) {
        Map<String, KeyValueService> delegateByNamespace = ImmutableMap.of();
        return create(delegates, delegateByTable, delegateByNamespace);
    }

    public static TableSplittingKeyValueService create(List<KeyValueService> delegates,
                                                       Map<String, KeyValueService> delegateByTable,
                                                       Map<String, KeyValueService> delegateByNamespace) {
        // See comment in get all table names for why we do this.
        Map<KeyValueService, Void> map = new IdentityHashMap<KeyValueService, Void>(delegates.size());
        for (KeyValueService delegate : delegates) {
            map.put(delegate, null);
        }
        Preconditions.checkArgument(map.keySet().containsAll(delegateByTable.values()),
                "delegateByTable must only have delegates from the delegate list");
        return new TableSplittingKeyValueService(delegates, delegateByTable, delegateByNamespace);
    }

    private final List<KeyValueService> delegates;
    private final Map<String, KeyValueService> delegateByTable;
    private final Map<String, KeyValueService> delegateByNamespace;

    private TableSplittingKeyValueService(List<KeyValueService> delegates,
                                         Map<String, KeyValueService> delegateByTable, Map<String, KeyValueService> delegateByNamespace) {
        this.delegates = ImmutableList.copyOf(delegates);
        this.delegateByTable = ImmutableMap.copyOf(delegateByTable);
        this.delegateByNamespace = ImmutableMap.copyOf(delegateByNamespace);
    }

    @Override
    public void addGarbageCollectionSentinelValues(String tableName, Set<Cell> cells) {
        getDelegate(tableName).addGarbageCollectionSentinelValues(tableName, cells);
    }

    @Override
    public void createTable(String tableName, byte[] tableMetadata) {
        getDelegate(tableName).createTable(tableName, tableMetadata);
    }

    @Override
    public void createTables(Map<String, byte[]> tableNamesToTableMetadata) {
        Map<KeyValueService, Map<String, byte[]>> splitTableNamesToTableMetadata = Maps.newHashMap();
        for (Entry<String, byte[]> tableEntry : tableNamesToTableMetadata.entrySet()) {
            String tableName = tableEntry.getKey();
            byte[] tableMetadata = tableEntry.getValue();

            KeyValueService delegate = getDelegate(tableName);
            if (splitTableNamesToTableMetadata.containsKey(delegate)) {
                splitTableNamesToTableMetadata.get(delegate).put(tableName, tableMetadata);
            } else {
                Map<String, byte[]> mapTableToMaxValue = Maps.newHashMap();
                mapTableToMaxValue.put(tableName, tableMetadata);
                splitTableNamesToTableMetadata.put(delegate, mapTableToMaxValue);
            }
        }
        for (KeyValueService delegate : splitTableNamesToTableMetadata.keySet()) {
            delegate.createTables(splitTableNamesToTableMetadata.get(delegate));
        }
    }

    @Override
    public void delete(String tableName, Multimap<Cell, Long> keys) {
        getDelegate(tableName).delete(tableName, keys);
    }

    @Override
    public void dropTable(String tableName) {
        getDelegate(tableName).dropTable(tableName);
    }

    @Override
    public void dropTables(Set<String> tableNames) {
        Map<KeyValueService, Set<String>> tablesByKVS = Maps.newHashMap();
        for (String tableName : tableNames) {
            KeyValueService delegate = getDelegate(tableName);
            if (tablesByKVS.containsKey(delegate)) {
                tablesByKVS.get(delegate).add(tableName);
            } else {
                Set<String> tablesBelongingToThisDelegate = Sets.newHashSet(tableName);
                tablesByKVS.put(delegate, tablesBelongingToThisDelegate);
            }
        }

        for (Entry<KeyValueService, Set<String>> kvsEntry : tablesByKVS.entrySet()) {
            kvsEntry.getKey().dropTables(kvsEntry.getValue());
        }
    }

    @Override
    public Map<Cell, Value> get(String tableName, Map<Cell, Long> timestampByCell) {
        return getDelegate(tableName).get(tableName, timestampByCell);
    }

    @Override
    public Set<String> getAllTableNames() {
        Set<String> ret = Sets.newHashSet();
        for (KeyValueService delegate : delegates) {
            Set<String> tableNames = delegate.getAllTableNames();
            for (String tableName : tableNames) {
                // Note: Can't use .equals!
                // If our delegate is a proxy (such as ProfilingProxy) it will delegate the equals
                // call. If the proxied class does not override equals, the instance comparison with
                // the proxy will fail. Fortunately, we really do want instance equality here.
                if (getDelegate(tableName) == delegate) {
                    ret.add(tableName);
                }
            }
        }
        return ret;
    }

    @Override
    public Multimap<Cell, Long> getAllTimestamps(String tableName, Set<Cell> cells, long timestamp) {
        return getDelegate(tableName).getAllTimestamps(tableName, cells, timestamp);
    }

    private KeyValueService getDelegate(String tableName) {
        KeyValueService delegate = delegateByTable.get(tableName);
        if (delegate == null) { // no explicit table mapping
            if (TableReference.isFullyQualifiedName(tableName)) { // try for a namespace mapping
                TableReference tableRef = TableReference.createFromFullyQualifiedName(tableName);
                delegate = delegateByNamespace.get(tableRef.getNamespace());
                if (delegate == null) { // namespace mapping did not cover this particular namespace
                    delegate = delegates.get(0);
                }
            } else { // not table-mapped and not a namespaced table, default back to primary split
                delegate = delegates.get(0);
            }
        }
        return delegate;
    }

    @Override
    public Collection<? extends KeyValueService> getDelegates() {
        return delegates;
    }

    @Override
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(String tableName,
                                                                                                           Iterable<RangeRequest> rangeRequests,
                                                                                                           long timestamp) {
        return getDelegate(tableName).getFirstBatchForRanges(tableName, rangeRequests, timestamp);
    }

    @Override
    public Map<Cell, Long> getLatestTimestamps(String tableName, Map<Cell, Long> timestampByCell) {
        return getDelegate(tableName).getLatestTimestamps(tableName, timestampByCell);
    }

    @Override
    public byte[] getMetadataForTable(String tableName) {
        return getDelegate(tableName).getMetadataForTable(tableName);
    }

    @Override
    public Map<String, byte[]> getMetadataForTables() {
        Map<String, byte[]> crossDelegateTableMetadata = Maps.newHashMap();
        for (KeyValueService delegate : getDelegates()) {
            crossDelegateTableMetadata.putAll(delegate.getMetadataForTables());
        }
        return crossDelegateTableMetadata;
    }

    @Override
    public ClosableIterator<RowResult<Value>> getRange(String tableName,
                                                       RangeRequest rangeRequest,
                                                       long timestamp) {
        return getDelegate(tableName).getRange(tableName, rangeRequest, timestamp);
    }

    @Override
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(String tableName,
                                                                       RangeRequest rangeRequest,
                                                                       long timestamp) {
        return getDelegate(tableName).getRangeOfTimestamps(tableName, rangeRequest, timestamp);
    }

    @Override
    public ClosableIterator<RowResult<Set<Value>>> getRangeWithHistory(String tableName,
                                                                       RangeRequest rangeRequest,
                                                                       long timestamp) {
        return getDelegate(tableName).getRangeWithHistory(tableName, rangeRequest, timestamp);
    }

    @Override
    public Map<Cell, Value> getRows(String tableName,
                                    Iterable<byte[]> rows,
                                    ColumnSelection columnSelection,
                                    long timestamp) {
        return getDelegate(tableName).getRows(tableName, rows, columnSelection, timestamp);
    }

    @Override
    public void initializeFromFreshInstance() {
        for (KeyValueService delegate : delegates) {
            delegate.initializeFromFreshInstance();
        }
    }

    @Override
    public void multiPut(Map<String, ? extends Map<Cell, byte[]>> valuesByTable, long timestamp) {
        Map<KeyValueService, Map<String, Map<Cell, byte[]>>> mapByDelegate = Maps.newHashMap();
        for (Entry<String, ? extends Map<Cell, byte[]>> e : valuesByTable.entrySet()) {
            KeyValueService delegate = getDelegate(e.getKey());
            Map<String, Map<Cell, byte[]>> map = mapByDelegate.get(delegate);
            if (map == null) {
                map = Maps.newHashMap();
                mapByDelegate.put(delegate, map);
            }
            map.put(e.getKey(), e.getValue());
        }
        for (Entry<KeyValueService, Map<String, Map<Cell, byte[]>>> e : mapByDelegate.entrySet()) {
            e.getKey().multiPut(e.getValue(), timestamp);
        }
    }

    @Override
    public void put(String tableName, Map<Cell, byte[]> values, long timestamp) {
        getDelegate(tableName).put(tableName, values, timestamp);
    }

    @Override
    public void putMetadataForTable(String tableName, byte[] metadata) {
        getDelegate(tableName).putMetadataForTable(tableName, metadata);
    }

    @Override
    public void putMetadataForTables(Map<String, byte[]> tableNameToMetadata) {
        Map<KeyValueService, Map<String, byte[]>> splitTableNameToMetadata = Maps.newHashMap();
        for (Entry<String, byte[]> tableEntry : tableNameToMetadata.entrySet()) {
            String tableName = tableEntry.getKey();
            byte[] metadata = tableEntry.getValue();

            KeyValueService delegate = getDelegate(tableName);
            if (splitTableNameToMetadata.containsKey(delegate)) {
                splitTableNameToMetadata.get(delegate).put(tableName, metadata);
            } else {
                Map<String, byte[]> mapTableToMetadata = Maps.newHashMap();
                mapTableToMetadata.put(tableName, metadata);
                splitTableNameToMetadata.put(delegate, mapTableToMetadata);
            }
        }
        for (KeyValueService delegate : splitTableNameToMetadata.keySet()) {
            delegate.putMetadataForTables(splitTableNameToMetadata.get(delegate));
        }
    }

    @Override
    public void putUnlessExists(String tableName, Map<Cell, byte[]> values)
            throws KeyAlreadyExistsException {
        getDelegate(tableName).putUnlessExists(tableName, values);
    }

    @Override
    public void putWithTimestamps(String tableName, Multimap<Cell, Value> values) {
        getDelegate(tableName).putWithTimestamps(tableName, values);
    }

    @Override
    public void close() {
        for (KeyValueService delegate : delegates) {
            delegate.close();
        }
    }

    @Override
    public void teardown() {
        for (KeyValueService delegate : delegates) {
            delegate.teardown();
        }
    }

    @Override
    public void truncateTable(String tableName){
        getDelegate(tableName).truncateTable(tableName);
    }

    @Override
    public void truncateTables(Set<String> tableNames) {
        SetMultimap<KeyValueService, String> tablesToTruncateByKvs = HashMultimap.create();
        for (String tableName : tableNames) {
            tablesToTruncateByKvs.put(getDelegate(tableName), tableName);
        }
        for (KeyValueService kvs : tablesToTruncateByKvs.keySet()) {
            kvs.truncateTables(tablesToTruncateByKvs.get(kvs));
        }
    }

    @Override
    public void compactInternally(String tableName) {
        getDelegate(tableName).compactInternally(tableName);
    }
}
