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
import java.util.Set;

import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.annotation.Idempotent;
import com.palantir.common.annotation.NonIdempotent;
import com.palantir.common.base.ClosableIterator;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

public class ThrowingKeyValueService implements KeyValueService {

    private final String errorMessage;
    private final Exception exception;

    public ThrowingKeyValueService(String errorMessage) {
        this.errorMessage = errorMessage;
        this.exception = new Exception("Provided for stack trace");
    }

    public ThrowingKeyValueService(Exception e) {
        this.errorMessage = "Unable to create a key value service with the given preferences. " +
                "This error was captured and delayed until now when the key value service was used. " +
                "Consult the AtlasDB documentation on CHANGING DATABASE CREDENTIALS AND OTHER PARAMETERS.";
        this.exception = e;
    }

    public IllegalStateException throwEx() {
        throw new IllegalStateException(errorMessage, exception);
    }

    @Override
    public void initializeFromFreshInstance() {
        // do nothing
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public void teardown() {
        // do nothing
    }

    @Override
    public Collection<? extends KeyValueService> getDelegates() {
        throw throwEx();
    }

    @Override
    @Idempotent
    public Map<Cell, Value> getRows(String tableName,
                                    Iterable<byte[]> rows,
                                    ColumnSelection columnSelection,
                                    long timestamp) {
        throw throwEx();
    }

    @Override
    @Idempotent
    public Map<Cell, Value> get(String tableName, Map<Cell, Long> timestampByCell) {
        throw throwEx();
    }

    @Override
    @Idempotent
    public Map<Cell, Long> getLatestTimestamps(String tableName, Map<Cell, Long> timestampByCell) {
        throw throwEx();
    }

    @Override
    @NonIdempotent
    public void put(String tableName, Map<Cell, byte[]> values, long timestamp) {
        throw throwEx();
    }

    @Override
    @NonIdempotent
    public void multiPut(Map<String, ? extends Map<Cell, byte[]>> valuesByTable, long timestamp) {
        throw throwEx();
    }

    @Override
    @NonIdempotent
    public void putWithTimestamps(String tableName, Multimap<Cell, Value> values) {
        throw throwEx();
    }

    @Override
    public void putUnlessExists(String tableName, Map<Cell, byte[]> values) throws KeyAlreadyExistsException {
        throw throwEx();
    }

    @Override
    @Idempotent
    public void delete(String tableName, Multimap<Cell, Long> keys) {
        throw throwEx();
    }

    @Override
    @Idempotent
    public void truncateTable(String tableName) {
        throw throwEx();
    }

    @Override
    @Idempotent
    public void truncateTables(Set<String> tableNames) {
        throw throwEx();
    }

    @Override
    @Idempotent
    public ClosableIterator<RowResult<Value>> getRange(String tableName,
                                                       RangeRequest rangeRequest,
                                                       long timestamp) {
        throw throwEx();
    }

    @Override
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(String tableName,
                                                                       RangeRequest rangeRequest,
                                                                       long timestamp) {
        throw throwEx();
    }

    @Override
    public ClosableIterator<RowResult<Set<Value>>> getRangeWithHistory(String tableName,
                                                                       RangeRequest rangeRequest,
                                                                       long timestamp) {
        throw throwEx();
    }

    @Override
    @Idempotent
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(String tableName,
                                                                                                           Iterable<RangeRequest> rangeRequests,
                                                                                                           long timestamp) {
        throw throwEx();
    }

    @Override
    @Idempotent
    public void dropTable(String tableName) {
        throw throwEx();
    }

    @Override
    @Idempotent
    public void dropTables(Set<String> tableNames) {
        throw throwEx();
    }

    @Override
    @Idempotent
    public void createTable(String tableName, byte[] tableMetadata) {
        throw throwEx();
    }

    @Override
    @Idempotent
    public Set<String> getAllTableNames() {
        throw throwEx();
    }

    @Override
    @Idempotent
    public byte[] getMetadataForTable(String tableName) {
        throw throwEx();
    }

    @Override
    @Idempotent
    public Map<String, byte[]> getMetadataForTables() {
        throw throwEx();
    }

    @Override
    @Idempotent
    public void putMetadataForTable(String tableName, byte[] metadata) {
        throw throwEx();
    }

    @Override
    @Idempotent
    public void addGarbageCollectionSentinelValues(String tableName, Set<Cell> cells) {
        throw throwEx();
    }

    @Override
    @Idempotent
    public Multimap<Cell, Long> getAllTimestamps(String tableName, Set<Cell> cells, long timestamp) {
        throw throwEx();
    }

    @Override
    public void createTables(Map<String, byte[]> tableNameToTableMetadata) {
        throw throwEx();
    }

    @Override
    public void putMetadataForTables(Map<String, byte[]> tableNameToMetadata) {
        throw throwEx();
    }

    @Override
    public void compactInternally(String tableName) {
        throw throwEx();
    }
}
