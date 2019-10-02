/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.migration;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.palantir.async.initializer.Callback;
import com.palantir.async.initializer.CallbackInitializable;
import com.palantir.atlasdb.coordination.CoordinationService;
import com.palantir.atlasdb.coordination.CoordinationServiceImpl;
import com.palantir.atlasdb.coordination.CoordinationStore;
import com.palantir.atlasdb.coordination.keyvalue.KeyValueServiceCoordinationStore;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetCompatibility;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.ClusterAvailabilityStatus;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.ImmutableCheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.TimestampRangeDelete;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.exception.AtlasDbDependencyException;
import com.palantir.common.streams.KeyedStream;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

public class TableMigratingKeyValueService implements KeyValueService, CallbackInitializable<TransactionManager> {
    private final KeyValueService delegate;
    private volatile MigratingTableMapperServiceImpl tableMapper;
    private final Set<TableReference> tablesToMigrate;

    public TableMigratingKeyValueService(KeyValueService delegate, MigratingTableMapperServiceImpl tableMapper,
            Set<TableReference> tablesToMigrate) {
        this.delegate = delegate;
        this.tableMapper = tableMapper;
        this.tablesToMigrate = tablesToMigrate;
    }

    public static KvsWithCallback create(KeyValueService kvs, Set<TableReference> migrate,
            LongSupplier immutableTsSupplier) {
        if (migrate.isEmpty()) {
            return ImmutableKvsWithCallback.of(kvs, Callback.noOp());
        }
        TableMigratingKeyValueService migratingKvs = new TableMigratingKeyValueService(kvs,
                new MigratingTableMapperServiceImpl(migrate, immutableTsSupplier), migrate);
        return ImmutableKvsWithCallback.of(migratingKvs, migratingKvs.singleAttemptCallback());
    }

    /**
     * READS always map to a single table.
     */

    @Override
    public Map<Cell, Value> getRows(TableReference tableRef, Iterable<byte[]> rows, ColumnSelection columnSelection,
            long timestamp) {
        return delegate.getRows(tableMapper.readTable(tableRef), rows, columnSelection, timestamp);
    }

    @Override
    public Map<byte[], RowColumnRangeIterator> getRowsColumnRange(TableReference tableRef, Iterable<byte[]> rows,
            BatchColumnRangeSelection selection, long timestamp) {
        return delegate.getRowsColumnRange(tableMapper.readTable(tableRef), rows, selection, timestamp);
    }

    @Override
    public RowColumnRangeIterator getRowsColumnRange(TableReference tableRef, Iterable<byte[]> rows,
            ColumnRangeSelection selection, int cellBatchHint, long timestamp) {
        return delegate.getRowsColumnRange(tableMapper.readTable(tableRef), rows, selection, cellBatchHint, timestamp);
    }

    @Override
    public Map<Cell, Value> get(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        return delegate.get(tableMapper.readTable(tableRef), timestampByCell);
    }

    @Override
    public Map<Cell, Long> getLatestTimestamps(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        return delegate.getLatestTimestamps(tableMapper.readTable(tableRef), timestampByCell);
    }

    @Override
    public ClosableIterator<RowResult<Value>> getRange(TableReference tableRef, RangeRequest rangeRequest, long ts) {
        return delegate.getRange(tableMapper.readTable(tableRef), rangeRequest, ts);
    }

    @Override
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(TableReference tableRef,
            RangeRequest rangeRequest, long timestamp) throws InsufficientConsistencyException {
        return delegate.getRangeOfTimestamps(tableMapper.readTable(tableRef), rangeRequest, timestamp);
    }

    @Override
    public ClosableIterator<List<CandidateCellForSweeping>> getCandidateCellsForSweeping(TableReference tableRef,
            CandidateCellForSweepingRequest request) {
        return delegate.getCandidateCellsForSweeping(tableMapper.readTable(tableRef), request);
    }

    @Override
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(
            TableReference tableRef, Iterable<RangeRequest> rangeRequests, long timestamp) {
        return delegate.getFirstBatchForRanges(tableMapper.readTable(tableRef), rangeRequests, timestamp);
    }

    @Override
    public byte[] getMetadataForTable(TableReference tableRef) {
        return delegate.getMetadataForTable(tableMapper.readTable(tableRef));
    }

    @Override
    public Multimap<Cell, Long> getAllTimestamps(TableReference tableRef, Set<Cell> cells, long timestamp)
            throws AtlasDbDependencyException {
        return delegate.getAllTimestamps(tableMapper.readTable(tableRef), cells, timestamp);
    }

    /**
     * WRITES potentially delegate to multiple tables.
     */

    @Override
    public void put(TableReference tableRef, Map<Cell, byte[]> values, long ts) throws KeyAlreadyExistsException {
        delegate.multiPut(asMapForWrites(tableRef, values), ts);
    }

    @Override
    public void multiPut(Map<TableReference, ? extends Map<Cell, byte[]>> valuesByTable, long timestamp)
            throws KeyAlreadyExistsException {
        delegate.multiPut(mapRemappingForWrites(valuesByTable), timestamp);
    }

    @Override
    public void putWithTimestamps(TableReference tableRef, Multimap<Cell, Value> cellValues)
            throws KeyAlreadyExistsException {
        tableMapper.writeTables(tableRef).forEach(table -> delegate.putWithTimestamps(table, cellValues));
    }

    @Override
    public void delete(TableReference tableRef, Multimap<Cell, Long> keys) {
        tableMapper.writeTables(tableRef).forEach(table -> delegate.delete(table, keys));
    }

    @Override
    public void deleteRange(TableReference tableRef, RangeRequest range) {
        tableMapper.writeTables(tableRef).forEach(table -> delegate.deleteRange(tableRef, range));
    }

    @Override
    public void deleteRows(TableReference tableRef, Iterable<byte[]> rows) {
        tableMapper.writeTables(tableRef).forEach(table -> delegate.deleteRows(table, rows));
    }

    @Override
    public void deleteAllTimestamps(TableReference tableRef, Map<Cell, TimestampRangeDelete> deletes)
            throws InsufficientConsistencyException {
        tableMapper.writeTables(tableRef).forEach(table -> delegate.deleteAllTimestamps(table, deletes));
    }

    @Override
    public void truncateTable(TableReference tableRef) throws InsufficientConsistencyException {
        delegate.truncateTables(tableMapper.writeTables(tableRef));
    }

    @Override
    public void truncateTables(Set<TableReference> tableRefs) throws InsufficientConsistencyException {
        delegate.truncateTables(setRemappingForWrites(tableRefs));
    }

    @Override
    public void dropTable(TableReference tableRef) throws InsufficientConsistencyException {
        delegate.dropTables(tableMapper.writeTables(tableRef));
    }

    @Override
    public void dropTables(Set<TableReference> tableRefs) throws InsufficientConsistencyException {
        delegate.dropTables(setRemappingForWrites(tableRefs));
    }

    @Override
    public void createTable(TableReference tableRef, byte[] tableMetadata) throws InsufficientConsistencyException {
        delegate.createTables(asMapForWrites(tableRef, tableMetadata));
    }

    @Override
    public void createTables(Map<TableReference, byte[]> tableRefToTableMetadata)
            throws InsufficientConsistencyException {
        delegate.createTables(mapRemappingForWrites(tableRefToTableMetadata));
    }

    @Override
    public void putMetadataForTable(TableReference tableRef, byte[] metadata) {
        delegate.putMetadataForTables(asMapForWrites(tableRef, metadata));
    }

    @Override
    public void putMetadataForTables(Map<TableReference, byte[]> tableRefToMetadata) {
        delegate.putMetadataForTables(mapRemappingForWrites(tableRefToMetadata));
    }

    @Override
    public void addGarbageCollectionSentinelValues(TableReference tableRef, Iterable<Cell> cells) {
        tableMapper.writeTables(tableRef).forEach(table -> delegate.addGarbageCollectionSentinelValues(table, cells));
    }

    @Override
    public void compactInternally(TableReference tableRef) {
        tableMapper.writeTables(tableRef).forEach(delegate::compactInternally);
    }

    @Override
    public void compactInternally(TableReference tableRef, boolean inMaintenanceMode) {
        tableMapper.writeTables(tableRef).forEach(table -> delegate.compactInternally(table, inMaintenanceMode));
    }

    /**
     * ATOMIC WRITES are not supported for multiple tables at once.
     */

    @Override
    public void putUnlessExists(TableReference tableRef, Map<Cell, byte[]> values) throws KeyAlreadyExistsException {
        TableReference writeTable = Iterables.getOnlyElement(tableMapper.writeTables(tableRef));
        delegate.putUnlessExists(writeTable, values);
    }

    @Override
    public void checkAndSet(CheckAndSetRequest checkAndSetRequest) throws CheckAndSetException {
        TableReference writeTable = Iterables.getOnlyElement(tableMapper.writeTables(checkAndSetRequest.table()));
        CheckAndSetRequest remappedRequest = new ImmutableCheckAndSetRequest.Builder()
                .from(checkAndSetRequest)
                .table(writeTable)
                .build();
        delegate.checkAndSet(remappedRequest);
    }

    /**
     * Methods that do not use table mapping.
     */

    @Override
    public Set<TableReference> getAllTableNames() {
        return delegate.getAllTableNames();
    }

    @Override
    public Map<TableReference, byte[]> getMetadataForTables() {
        return delegate.getMetadataForTables();
    }

    @Override
    public Collection<? extends KeyValueService> getDelegates() {
        return ImmutableList.of(delegate);
    }

    @Override
    public boolean supportsCheckAndSet() {
        return delegate.supportsCheckAndSet();
    }

    @Override
    public CheckAndSetCompatibility getCheckAndSetCompatibility() {
        return delegate.getCheckAndSetCompatibility();
    }

    @Override
    public ClusterAvailabilityStatus getClusterAvailabilityStatus() {
        return delegate.getClusterAvailabilityStatus();
    }

    @Override
    public boolean isInitialized() {
        return delegate.isInitialized();
    }

    @Override
    public boolean performanceIsSensitiveToTombstones() {
        return delegate.performanceIsSensitiveToTombstones();
    }

    @Override
    public boolean shouldTriggerCompactions() {
        return delegate.shouldTriggerCompactions();
    }

    @Override
    public void close() {
        delegate.close();
    }

    private <T> Map<TableReference, T> asMapForWrites(TableReference tableRef, T values) {
        return  tableMapper.writeTables(tableRef).stream()
                .collect(Collectors.toMap(table -> table, each -> values));
    }

    private <T> Map<TableReference, T> mapRemappingForWrites(Map<TableReference, T> originalMap) {
        return KeyedStream.stream(originalMap)
                .mapKeys(tableMapper::writeTables)
                .flatMapKeys(Set::stream)
                .collectToMap();
    }

    private Set<TableReference> setRemappingForWrites(Set<TableReference> tableRefs) {
        return tableRefs.stream()
                .map(tableMapper::writeTables)
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
    }

    @Override
    public void initialize(TransactionManager transactionManager) {
        CoordinationStore<TableMigrationStateMap> store = KeyValueServiceCoordinationStore.create(
                new ObjectMapper(),
                this,
                PtBytes.toBytes("migrationRow"),
                () -> transactionManager.getTimelockService().getFreshTimestamp(),
                Objects::equal,
                TableMigrationStateMap.class,
                false);
        CoordinationService<TableMigrationStateMap> coordinator = new CoordinationServiceImpl<>(store);

        MigrationCoordinationService coordinationService = MigrationCoordinationServiceImpl.create(coordinator);
        tableMapper.initialize(coordinator);
        MigratorState migrator = MigratorState.createAndRun(transactionManager, coordinationService, tablesToMigrate);
    }
}
