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
package com.palantir.atlasdb.keyvalue;

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
import com.palantir.atlasdb.schema.TableReference;
import com.palantir.common.annotation.Idempotent;
import com.palantir.common.annotation.NonIdempotent;
import com.palantir.common.base.ClosableIterator;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

/**
 * Version of KeyValueService using TableReference (namespace plus tablename) instead of tableName.
 */
public interface NamespacedKeyValueService {
    Collection<? extends KeyValueService> getDelegates();

    void initializeFromFreshInstance();

    void close();

    void teardown();

    TableMappingService getTableMapper();

    @Idempotent
    Map<Cell, Value> getRows(TableReference tableRef, Iterable<byte[]> rows,
                             ColumnSelection columnSelection,long timestamp);

    @Idempotent
    Map<Cell, Value> get(TableReference tableRef, Map<Cell, Long> timestampByCell);

    @Idempotent
    Map<Cell, Long> getLatestTimestamps(TableReference tableRef, Map<Cell, Long> timestampByCell);

    @NonIdempotent
    void put(TableReference tableRef, Map<Cell, byte[]> values, long timestamp);

    @NonIdempotent
    void multiPut(Map<TableReference, ? extends Map<Cell, byte[]>> valuesByTable, long timestamp);

    @NonIdempotent
    void putWithTimestamps(TableReference tableRef, Multimap<Cell, Value> values);

    void putUnlessExists(TableReference tableRef, Map<Cell, byte[]> values) throws KeyAlreadyExistsException;

    @Idempotent
    void delete(TableReference tableRef, Multimap<Cell, Long> keys);

    @Idempotent
    void truncateTable(TableReference tableRef);

    @Idempotent
    void truncateTables(Set<TableReference> tableRefs);

    @Idempotent
    ClosableIterator<RowResult<Value>> getRange(TableReference tableRef,
                                                RangeRequest rangeRequest,
                                                long timestamp);

    @Idempotent
    ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(TableReference tableReference,
                                                                RangeRequest rangeRequest,
                                                                long timestamp);


    @Idempotent
    ClosableIterator<RowResult<Set<Value>>> getRangeWithHistory(TableReference tableReference,
                                                                RangeRequest rangeRequest,
                                                                long timestamp);

    @Idempotent
    Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(TableReference tableRef,
            Iterable<RangeRequest> rangeRequests,
            long timestamp);

    ////////////////////////////////////////////////////////////
    // TABLE CREATION AND METADATA
    ////////////////////////////////////////////////////////////

    @Idempotent
    void dropTable(TableReference tableRef);

    @Idempotent
    void dropTables(Set<TableReference> tableRef);

    @Idempotent
    void createTable(TableReference tableRef, byte[] tableMetadata);

    @Idempotent
    void createTables(Map<TableReference, byte[]> tableReferencesToTableMetadata);

    @Idempotent
    Set<TableReference> getAllTableNames();

    @Idempotent
    byte[] getMetadataForTable(TableReference tableRef);

    @Idempotent
    Map<TableReference, byte[]> getMetadataForTables();

    @Idempotent
    void putMetadataForTable(TableReference tableRef, byte[] metadata);

    @Idempotent
    void putMetadataForTables(Map<TableReference, byte[]> tableReferencesToMetadata);

    ////////////////////////////////////////////////////////////
    // METHODS TO SUPPORT GARBAGE COLLECTION
    ////////////////////////////////////////////////////////////

    @Idempotent
    void addGarbageCollectionSentinelValues(TableReference tableRef, Set<Cell> cells);

    @Idempotent
    Multimap<Cell, Long> getAllTimestamps(TableReference tableRef, Set<Cell> cells, long timestamp);

    void compactInternally(TableReference tableRef);
}
