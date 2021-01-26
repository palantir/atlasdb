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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
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
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.TimestampRangeDelete;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.base.ClosableIterator;
import com.palantir.logsafe.Preconditions;
import com.palantir.util.paging.TokenBackedBasicResultsPage;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/*
 * WARNING: Code assumes that this will be called above namespace mapping layer.
 *          Code also assumes that the more specific mapping (by table)
 *          trumps the more generic mapping (by namespace).
 */
public final class TableSplittingKeyValueService implements KeyValueService {
    public static TableSplittingKeyValueService create(
            List<KeyValueService> delegates, Map<TableReference, KeyValueService> delegateByTable) {
        Map<Namespace, KeyValueService> delegateByNamespace = ImmutableMap.of();
        return create(delegates, delegateByTable, delegateByNamespace);
    }

    public static TableSplittingKeyValueService create(
            List<KeyValueService> delegates,
            Map<TableReference, KeyValueService> delegateByTable,
            Map<Namespace, KeyValueService> delegateByNamespace) {
        // See comment in get all table names for why we do this.
        IdentityHashMap<KeyValueService, Void> map = new IdentityHashMap<KeyValueService, Void>(delegates.size());
        for (KeyValueService delegate : delegates) {
            map.put(delegate, null);
        }
        Preconditions.checkArgument(
                map.keySet().containsAll(delegateByTable.values()),
                "delegateByTable must only have delegates from the delegate list");
        return new TableSplittingKeyValueService(delegates, delegateByTable, delegateByNamespace);
    }

    private final List<KeyValueService> delegates;
    private final Map<TableReference, KeyValueService> delegateByTable;
    private final Map<Namespace, KeyValueService> delegateByNamespace;

    private TableSplittingKeyValueService(
            List<KeyValueService> delegates,
            Map<TableReference, KeyValueService> delegateByTable,
            Map<Namespace, KeyValueService> delegateByNamespace) {
        this.delegates = ImmutableList.copyOf(delegates);
        this.delegateByTable = ImmutableMap.copyOf(delegateByTable);
        this.delegateByNamespace = ImmutableMap.copyOf(delegateByNamespace);
    }

    @Override
    public void addGarbageCollectionSentinelValues(TableReference tableRef, Iterable<Cell> cells) {
        getDelegate(tableRef).addGarbageCollectionSentinelValues(tableRef, cells);
    }

    @Override
    public void createTable(TableReference tableRef, byte[] tableMetadata) {
        getDelegate(tableRef).createTable(tableRef, tableMetadata);
    }

    @Override
    public void createTables(Map<TableReference, byte[]> tableNamesToTableMetadata) {
        Map<KeyValueService, Map<TableReference, byte[]>> splitTableNamesToTableMetadata =
                groupByDelegate(tableNamesToTableMetadata);
        splitTableNamesToTableMetadata.forEach(KeyValueService::createTables);
    }

    private Map<KeyValueService, Map<TableReference, byte[]>> groupByDelegate(
            Map<TableReference, byte[]> tableRefsToTableMetadata) {
        Map<KeyValueService, Map<TableReference, byte[]>> splitTableNamesToTableMetadata = new HashMap<>();
        for (Map.Entry<TableReference, byte[]> tableEntry : tableRefsToTableMetadata.entrySet()) {
            TableReference tableRef = tableEntry.getKey();
            byte[] tableMetadata = tableEntry.getValue();

            KeyValueService delegate = getDelegate(tableRef);
            if (splitTableNamesToTableMetadata.containsKey(delegate)) {
                splitTableNamesToTableMetadata.get(delegate).put(tableRef, tableMetadata);
            } else {
                Map<TableReference, byte[]> mapTableToMaxValue = new HashMap<>();
                mapTableToMaxValue.put(tableRef, tableMetadata);
                splitTableNamesToTableMetadata.put(delegate, mapTableToMaxValue);
            }
        }
        return splitTableNamesToTableMetadata;
    }

    @Override
    public void delete(TableReference tableRef, Multimap<Cell, Long> keys) {
        getDelegate(tableRef).delete(tableRef, keys);
    }

    @Override
    public void deleteRange(TableReference tableRef, RangeRequest range) {
        getDelegate(tableRef).deleteRange(tableRef, range);
    }

    @Override
    public void deleteRows(TableReference tableRef, Iterable<byte[]> rows) {
        getDelegate(tableRef).deleteRows(tableRef, rows);
    }

    @Override
    public void deleteAllTimestamps(TableReference tableRef, Map<Cell, TimestampRangeDelete> deletes) {
        getDelegate(tableRef).deleteAllTimestamps(tableRef, deletes);
    }

    @Override
    public void dropTable(TableReference tableRef) {
        getDelegate(tableRef).dropTable(tableRef);
    }

    @Override
    public void dropTables(Set<TableReference> tableRefs) {
        Map<KeyValueService, Set<TableReference>> tablesByKvs = new HashMap<>();
        for (TableReference tableRef : tableRefs) {
            KeyValueService delegate = getDelegate(tableRef);
            if (tablesByKvs.containsKey(delegate)) {
                tablesByKvs.get(delegate).add(tableRef);
            } else {
                Set<TableReference> tablesBelongingToThisDelegate = Sets.newHashSet(tableRef);
                tablesByKvs.put(delegate, tablesBelongingToThisDelegate);
            }
        }

        for (Map.Entry<KeyValueService, Set<TableReference>> kvsEntry : tablesByKvs.entrySet()) {
            kvsEntry.getKey().dropTables(kvsEntry.getValue());
        }
    }

    @Override
    public Map<Cell, Value> get(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        return getDelegate(tableRef).get(tableRef, timestampByCell);
    }

    @Override
    public Set<TableReference> getAllTableNames() {
        Set<TableReference> ret = new HashSet<>();
        for (KeyValueService delegate : delegates) {
            Set<TableReference> tableRefs = delegate.getAllTableNames();
            for (TableReference tableRef : tableRefs) {
                // Note: Can't use .equals!
                // If our delegate is a proxy (such as ProfilingProxy) it will delegate the equals
                // call. If the proxied class does not override equals, the instance comparison with
                // the proxy will fail. Fortunately, we really do want instance equality here.
                if (getDelegate(tableRef) == delegate) {
                    ret.add(tableRef);
                }
            }
        }
        return ret;
    }

    @Override
    public Multimap<Cell, Long> getAllTimestamps(TableReference tableRef, Set<Cell> cells, long timestamp) {
        return getDelegate(tableRef).getAllTimestamps(tableRef, cells, timestamp);
    }

    public KeyValueService getDelegate(TableReference tableRef) {
        return tableDelegateFor(tableRef)
                .orElseGet(() -> namespaceDelegateFor(tableRef).orElseGet(() -> delegates.get(0)));
    }

    private Optional<KeyValueService> tableDelegateFor(TableReference tableRef) {
        return Optional.ofNullable(delegateByTable.get(tableRef));
    }

    private Optional<KeyValueService> namespaceDelegateFor(TableReference tableRef) {
        if (!tableRef.isFullyQualifiedName()) {
            return Optional.empty();
        }
        KeyValueService delegate = delegateByNamespace.get(tableRef.getNamespace());
        return Optional.ofNullable(delegate);
    }

    @Override
    public Collection<? extends KeyValueService> getDelegates() {
        return delegates;
    }

    @Override
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(
            TableReference tableRef, Iterable<RangeRequest> rangeRequests, long timestamp) {
        return getDelegate(tableRef).getFirstBatchForRanges(tableRef, rangeRequests, timestamp);
    }

    @Override
    public Map<Cell, Long> getLatestTimestamps(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        return getDelegate(tableRef).getLatestTimestamps(tableRef, timestampByCell);
    }

    @Override
    public byte[] getMetadataForTable(TableReference tableRef) {
        return getDelegate(tableRef).getMetadataForTable(tableRef);
    }

    @Override
    public Map<TableReference, byte[]> getMetadataForTables() {
        Map<TableReference, byte[]> crossDelegateTableMetadata = new HashMap<>();
        for (KeyValueService delegate : getDelegates()) {
            crossDelegateTableMetadata.putAll(delegate.getMetadataForTables());
        }
        return crossDelegateTableMetadata;
    }

    @Override
    public ClosableIterator<RowResult<Value>> getRange(
            TableReference tableRef, RangeRequest rangeRequest, long timestamp) {
        return getDelegate(tableRef).getRange(tableRef, rangeRequest, timestamp);
    }

    @Override
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(
            TableReference tableRef, RangeRequest rangeRequest, long timestamp) {
        return getDelegate(tableRef).getRangeOfTimestamps(tableRef, rangeRequest, timestamp);
    }

    @Override
    public ClosableIterator<List<CandidateCellForSweeping>> getCandidateCellsForSweeping(
            TableReference tableRef, CandidateCellForSweepingRequest request) {
        return getDelegate(tableRef).getCandidateCellsForSweeping(tableRef, request);
    }

    @Override
    public Map<Cell, Value> getRows(
            TableReference tableRef, Iterable<byte[]> rows, ColumnSelection columnSelection, long timestamp) {
        return getDelegate(tableRef).getRows(tableRef, rows, columnSelection, timestamp);
    }

    @Override
    public Map<byte[], RowColumnRangeIterator> getRowsColumnRange(
            TableReference tableRef,
            Iterable<byte[]> rows,
            BatchColumnRangeSelection batchColumnRangeSelection,
            long timestamp) {
        return getDelegate(tableRef).getRowsColumnRange(tableRef, rows, batchColumnRangeSelection, timestamp);
    }

    @Override
    public RowColumnRangeIterator getRowsColumnRange(
            TableReference tableRef,
            Iterable<byte[]> rows,
            ColumnRangeSelection columnRangeSelection,
            int cellBatchHint,
            long timestamp) {
        return getDelegate(tableRef).getRowsColumnRange(tableRef, rows, columnRangeSelection, cellBatchHint, timestamp);
    }

    @Override
    public void multiPut(Map<TableReference, ? extends Map<Cell, byte[]>> valuesByTable, long timestamp) {
        Map<KeyValueService, Map<TableReference, Map<Cell, byte[]>>> mapByDelegate = new HashMap<>();
        for (Map.Entry<TableReference, ? extends Map<Cell, byte[]>> e : valuesByTable.entrySet()) {
            KeyValueService delegate = getDelegate(e.getKey());
            Map<TableReference, Map<Cell, byte[]>> map =
                    mapByDelegate.computeIfAbsent(delegate, table -> new HashMap<>());
            map.put(e.getKey(), e.getValue());
        }
        for (Map.Entry<KeyValueService, Map<TableReference, Map<Cell, byte[]>>> e : mapByDelegate.entrySet()) {
            e.getKey().multiPut(e.getValue(), timestamp);
        }
    }

    @Override
    public void put(TableReference tableRef, Map<Cell, byte[]> values, long timestamp) {
        getDelegate(tableRef).put(tableRef, values, timestamp);
    }

    @Override
    public void putMetadataForTable(TableReference tableRef, byte[] metadata) {
        getDelegate(tableRef).putMetadataForTable(tableRef, metadata);
    }

    @Override
    public void putMetadataForTables(Map<TableReference, byte[]> tableRefToMetadata) {
        Map<KeyValueService, Map<TableReference, byte[]>> splitTableNameToMetadata =
                groupByDelegate(tableRefToMetadata);
        splitTableNameToMetadata.forEach(KeyValueService::putMetadataForTables);
    }

    @Override
    public void putUnlessExists(TableReference tableRef, Map<Cell, byte[]> values) throws KeyAlreadyExistsException {
        getDelegate(tableRef).putUnlessExists(tableRef, values);
    }

    @Override
    public CheckAndSetCompatibility getCheckAndSetCompatibility() {
        return CheckAndSetCompatibility.min(getDelegates().stream().map(KeyValueService::getCheckAndSetCompatibility));
    }

    @Override
    public void checkAndSet(CheckAndSetRequest checkAndSetRequest) {
        getDelegate(checkAndSetRequest.table()).checkAndSet(checkAndSetRequest);
    }

    @Override
    public void putWithTimestamps(TableReference tableRef, Multimap<Cell, Value> values) {
        getDelegate(tableRef).putWithTimestamps(tableRef, values);
    }

    @Override
    public void close() {
        for (KeyValueService delegate : delegates) {
            delegate.close();
        }
    }

    @Override
    public void truncateTable(TableReference tableRef) {
        getDelegate(tableRef).truncateTable(tableRef);
    }

    @Override
    public void truncateTables(Set<TableReference> tableRefs) {
        SetMultimap<KeyValueService, TableReference> tablesToTruncateByKvs = HashMultimap.create();
        for (TableReference tableRef : tableRefs) {
            tablesToTruncateByKvs.put(getDelegate(tableRef), tableRef);
        }
        for (KeyValueService kvs : tablesToTruncateByKvs.keySet()) {
            kvs.truncateTables(tablesToTruncateByKvs.get(kvs));
        }
    }

    @Override
    public void compactInternally(TableReference tableRef) {
        getDelegate(tableRef).compactInternally(tableRef);
    }

    @Override
    public void compactInternally(TableReference tableRef, boolean inMaintenanceMode) {
        getDelegate(tableRef).compactInternally(tableRef, inMaintenanceMode);
    }

    @Override
    public ClusterAvailabilityStatus getClusterAvailabilityStatus() {
        return delegates.stream()
                .map(KeyValueService::getClusterAvailabilityStatus)
                .min(Comparator.naturalOrder())
                .orElse(ClusterAvailabilityStatus.ALL_AVAILABLE);
    }

    @Override
    public boolean shouldTriggerCompactions() {
        return delegates.stream().anyMatch(KeyValueService::shouldTriggerCompactions);
    }

    @Override
    public ListenableFuture<Map<Cell, Value>> getAsync(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        return getDelegate(tableRef).getAsync(tableRef, timestampByCell);
    }
}
