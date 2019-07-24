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

package com.palantir.atlasdb.keyvalue.impl;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.coordination.CoordinationService;
import com.palantir.atlasdb.coordination.ValueAndBound;
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
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.TimestampRangeDelete;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.exception.AtlasDbDependencyException;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

public class TableMigratingKeyValueService implements KeyValueService {
    private final KeyValueService delegate;
    private final CoordinationService<MigrationsState> coordinationService;
    private final TableReference oldTableRef;
    private final TableReference newTableRef;

    public TableMigratingKeyValueService(KeyValueService delegate, CoordinationService<MigrationsState> coordinationService,
            TableReference oldTableRef, TableReference newTableRef) {
        this.delegate = delegate;
        this.coordinationService = coordinationService;
        this.oldTableRef = oldTableRef;
        this.newTableRef = newTableRef;
    }

    private TableReference readTable(TableReference tableRef, long timestamp) {
        MigrationsState currentState = getMigrationState(timestamp);
        if (currentState == MigrationsState.WRITE_SECOND_READ_SECOND && tableRef.equals(oldTableRef)) {
            return newTableRef;
        }
        return tableRef;
    }

    private List<TableReference> writeTables(TableReference tableRef, long timestamp) {
        if (!tableRef.equals(oldTableRef)) {
            return ImmutableList.of(tableRef);
        }
        MigrationsState currentState = getMigrationState(timestamp);
        switch (currentState) {
            case WRITE_FIRST_ONLY:
                return ImmutableList.of(oldTableRef);
            case WRITE_BOTH_READ_FIRST:
                return ImmutableList.of(oldTableRef, newTableRef);
            case WRITE_SECOND_READ_SECOND:
                return ImmutableList.of(newTableRef);
            default:
                throw new IllegalStateException("what are you doing");
        }
    }

    private MigrationsState getMigrationState(long timestamp) {
        return coordinationService.getValueForTimestamp(timestamp)
                .map(ValueAndBound::value)
                .map(Optional::get)
                .orElse(MigrationsState.WRITE_FIRST_ONLY);
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public Collection<? extends KeyValueService> getDelegates() {
        return ImmutableList.of(delegate);
    }

    @Override
    public Map<Cell, Value> getRows(TableReference tableRef, Iterable<byte[]> rows, ColumnSelection columnSelection,
            long timestamp) {
        return delegate.getRows(readTable(tableRef, timestamp), rows, columnSelection, timestamp);
    }

    @Override
    public Map<byte[], RowColumnRangeIterator> getRowsColumnRange(TableReference tableRef, Iterable<byte[]> rows,
            BatchColumnRangeSelection batchColumnRangeSelection, long timestamp) {
        return delegate.getRowsColumnRange(readTable(tableRef, timestamp), rows, batchColumnRangeSelection, timestamp);
    }

    @Override
    public RowColumnRangeIterator getRowsColumnRange(TableReference tableRef, Iterable<byte[]> rows,
            ColumnRangeSelection columnRangeSelection, int cellBatchHint, long timestamp) {
        return delegate.getRowsColumnRange(readTable(tableRef, timestamp), rows, columnRangeSelection, cellBatchHint, timestamp);
    }

    @Override
    public Map<Cell, Value> get(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        return delegate.get(tableRef, timestampByCell);
    }

    @Override
    public Map<Cell, Long> getLatestTimestamps(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        return null;
    }

    @Override
    public void put(TableReference tableRef, Map<Cell, byte[]> values, long timestamp)
            throws KeyAlreadyExistsException {

    }

    @Override
    public void multiPut(Map<TableReference, ? extends Map<Cell, byte[]>> valuesByTable, long timestamp)
            throws KeyAlreadyExistsException {

    }

    @Override
    public void putWithTimestamps(TableReference tableRef, Multimap<Cell, Value> cellValues)
            throws KeyAlreadyExistsException {

    }

    @Override
    public void putUnlessExists(TableReference tableRef, Map<Cell, byte[]> values) throws KeyAlreadyExistsException {

    }

    @Override
    public boolean supportsCheckAndSet() {
        return false;
    }

    @Override
    public CheckAndSetCompatibility getCheckAndSetCompatibility() {
        return null;
    }

    @Override
    public void checkAndSet(CheckAndSetRequest checkAndSetRequest) throws CheckAndSetException {

    }

    @Override
    public void delete(TableReference tableRef, Multimap<Cell, Long> keys) {

    }

    @Override
    public void deleteRange(TableReference tableRef, RangeRequest range) {

    }

    @Override
    public void deleteRows(TableReference tableRef, Iterable<byte[]> rows) {

    }

    @Override
    public void deleteAllTimestamps(TableReference tableRef, Map<Cell, TimestampRangeDelete> deletes)
            throws InsufficientConsistencyException {

    }

    @Override
    public void truncateTable(TableReference tableRef) throws InsufficientConsistencyException {

    }

    @Override
    public void truncateTables(Set<TableReference> tableRefs) throws InsufficientConsistencyException {

    }

    @Override
    public ClosableIterator<RowResult<Value>> getRange(TableReference tableRef, RangeRequest rangeRequest,
            long timestamp) {
        return null;
    }

    @Override
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(TableReference tableRef,
            RangeRequest rangeRequest, long timestamp) throws InsufficientConsistencyException {
        return null;
    }

    @Override
    public ClosableIterator<List<CandidateCellForSweeping>> getCandidateCellsForSweeping(TableReference tableRef,
            CandidateCellForSweepingRequest request) {
        return null;
    }

    @Override
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(
            TableReference tableRef, Iterable<RangeRequest> rangeRequests, long timestamp) {
        return null;
    }

    @Override
    public void dropTable(TableReference tableRef) throws InsufficientConsistencyException {

    }

    @Override
    public void dropTables(Set<TableReference> tableRefs) throws InsufficientConsistencyException {

    }

    @Override
    public void createTable(TableReference tableRef, byte[] tableMetadata) throws InsufficientConsistencyException {

    }

    @Override
    public void createTables(Map<TableReference, byte[]> tableRefToTableMetadata)
            throws InsufficientConsistencyException {

    }

    @Override
    public Set<TableReference> getAllTableNames() {
        return null;
    }

    @Override
    public byte[] getMetadataForTable(TableReference tableRef) {
        return new byte[0];
    }

    @Override
    public Map<TableReference, byte[]> getMetadataForTables() {
        return null;
    }

    @Override
    public void putMetadataForTable(TableReference tableRef, byte[] metadata) {

    }

    @Override
    public void putMetadataForTables(Map<TableReference, byte[]> tableRefToMetadata) {

    }

    @Override
    public void addGarbageCollectionSentinelValues(TableReference tableRef, Iterable<Cell> cells) {

    }

    @Override
    public Multimap<Cell, Long> getAllTimestamps(TableReference tableRef, Set<Cell> cells, long timestamp)
            throws AtlasDbDependencyException {
        return null;
    }

    @Override
    public void compactInternally(TableReference tableRef) {

    }

    @Override
    public void compactInternally(TableReference tableRef, boolean inMaintenanceMode) {

    }

    @Override
    public ClusterAvailabilityStatus getClusterAvailabilityStatus() {
        return null;
    }

    @Override
    public boolean isInitialized() {
        return false;
    }

    @Override
    public boolean performanceIsSensitiveToTombstones() {
        return false;
    }

    @Override
    public boolean shouldTriggerCompactions() {
        return false;
    }


    public enum MigrationsState {
        WRITE_FIRST_ONLY,
        WRITE_BOTH_READ_FIRST,
        WRITE_BOTH_READ_SECOND,
        WRITE_SECOND_READ_SECOND
    }

}
