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

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

/**
 * This wrapper KVS should ensure that we're consistent across KVSs with:
 *   - How we apply tricks to avoid work when people make fairly stupid queries
 *     (i.e. call out to the database to read in nothing, etc)
 *   - How we validate query inputs for sanity / correctness
 *   - Not having a ton of boilerplate
 *
 * @author clockfort
 */
public class ValidatingQueryRewritingKeyValueService extends ForwardingKeyValueService implements KeyValueService {
    private static final Logger log = LoggerFactory.getLogger(ValidatingQueryRewritingKeyValueService.class);
    private static String TRANSACTION_ERROR = "shouldn't be putting into the transaction table at this level of KVS abstraction";

    public static ValidatingQueryRewritingKeyValueService create(KeyValueService delegate) {
        return new ValidatingQueryRewritingKeyValueService(delegate);
    }

    private final KeyValueService delegate;

    protected ValidatingQueryRewritingKeyValueService(KeyValueService delegate) {
        this.delegate = delegate;
    }

    @Override
    protected KeyValueService delegate() {
        return delegate;
    }

    @Override
    public void createTable(TableReference tableRef, byte[] tableMetadata) {
        sanityCheckTableName(tableRef);
        delegate.createTable(tableRef, tableMetadata);
    }

    @Override
    public void createTables(Map<TableReference, byte[]> tableRefToTableMetadata) {
        if (tableRefToTableMetadata.isEmpty()) {
            return;
        }
        if (tableRefToTableMetadata.size() == 1) {
            Entry<TableReference, byte[]> element = Iterables.getOnlyElement(tableRefToTableMetadata.entrySet());
            createTable(element.getKey(), element.getValue());
            return;
        }
        for (TableReference tableRef : tableRefToTableMetadata.keySet()) {
            sanityCheckTableName(tableRef);
        }
        delegate.createTables(tableRefToTableMetadata);
    }

    protected void sanityCheckTableName(TableReference tableRef) {
        String tableName = tableRef.getQualifiedName();
        Validate.isTrue(
                (!tableName.startsWith("_") && tableName.contains("."))
                        || AtlasDbConstants.HIDDEN_TABLES.contains(tableRef)
                        || tableName.startsWith(AtlasDbConstants.NAMESPACE_PREFIX),
                "invalid tableName: " + tableName);
    }

    @Override
    public void delete(TableReference tableRef, Multimap<Cell, Long> keys) {
        if (keys.isEmpty()) {
            return;
        }
        delegate.delete(tableRef, keys);
    }

    @Override
    public Map<Cell, Value> get(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        if (timestampByCell.isEmpty()) {
            return ImmutableMap.of();
        }
        return delegate.get(tableRef, timestampByCell);
    }

    @Override
    public Multimap<Cell, Long> getAllTimestamps(TableReference tableRef, Set<Cell> cells, long timestamp) {
        if (cells.isEmpty()) {
            return ImmutableSetMultimap.of();
        } else {
            return delegate.getAllTimestamps(tableRef, cells, timestamp);
        }
    }

    @Override
    public Collection<? extends KeyValueService> getDelegates() {
        return ImmutableList.of(delegate);
    }

    @Override
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(TableReference tableRef, Iterable<RangeRequest> rangeRequests, long timestamp) {
        if (Iterables.isEmpty(rangeRequests)) {
            return ImmutableMap.of();
        }
        return delegate.getFirstBatchForRanges(tableRef, rangeRequests, timestamp);
    }

    @Override
    public Map<Cell, Long> getLatestTimestamps(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        if (timestampByCell.isEmpty()) {
            return ImmutableMap.of();
        }
        return delegate.getLatestTimestamps(tableRef, timestampByCell);
    }

    @Override
    public Map<Cell, Value> getRows(TableReference tableRef, Iterable<byte[]> rows, ColumnSelection columnSelection, long timestamp) {
        if (Iterables.isEmpty(rows) || columnSelection.noColumnsSelected()) {
            return ImmutableMap.of();
        }
        return delegate.getRows(tableRef, rows, columnSelection, timestamp);
    }

    @Override
    public void multiPut(Map<TableReference, ? extends Map<Cell, byte[]>> valuesByTable, long timestamp) throws KeyAlreadyExistsException {
        if (valuesByTable.isEmpty()) {
            return;
        }
        if (valuesByTable.size() == 1) {
            Entry<TableReference, ? extends Map<Cell, byte[]>> entry = Iterables.getOnlyElement(valuesByTable.entrySet());
            put(entry.getKey(), entry.getValue(), timestamp);
            return;
        }
        delegate.multiPut(valuesByTable, timestamp);
    }

    @Override
    public void put(TableReference tableRef, Map<Cell, byte[]> values, long timestamp) throws KeyAlreadyExistsException {
        Validate.isTrue(timestamp != Long.MAX_VALUE);
        Validate.isTrue(timestamp >= 0);
        Validate.isTrue(!tableRef.equals(TransactionConstants.TRANSACTION_TABLE), TRANSACTION_ERROR);
        if (values.isEmpty()) {
            return;
        }
        delegate.put(tableRef, values, timestamp);
    }

    @Override
    public void putMetadataForTables(Map<TableReference, byte[]> tableRefToMetadata) {
        if (tableRefToMetadata.isEmpty()) {
            return;
        }
        if (tableRefToMetadata.size() == 1) {
            Entry<TableReference, byte[]> entry = Iterables.getOnlyElement(tableRefToMetadata.entrySet());
            putMetadataForTable(entry.getKey(), entry.getValue());
            return;
        }
        delegate.putMetadataForTables(tableRefToMetadata);
    }

    @Override
    public void putUnlessExists(TableReference tableRef, Map<Cell, byte[]> values) throws KeyAlreadyExistsException {
        if (values.isEmpty()) {
            return;
        }
        delegate.putUnlessExists(tableRef, values);
    }

    @Override
    public void putWithTimestamps(TableReference tableRef, Multimap<Cell, Value> cellValues) throws KeyAlreadyExistsException {
        if (cellValues.isEmpty()) {
            return;
        }
        Validate.isTrue(!tableRef.equals(TransactionConstants.TRANSACTION_TABLE), TRANSACTION_ERROR);

        long lastTimestamp = -1;
        boolean allAtSameTimestamp = true;
        for (Value value : cellValues.values()) {
            long timestamp = value.getTimestamp();
            Validate.isTrue(timestamp != Long.MAX_VALUE);
            Validate.isTrue(timestamp >= 0);
            if (lastTimestamp != -1 && timestamp != lastTimestamp) {
                allAtSameTimestamp = false;
            }
            lastTimestamp = timestamp;
        }

        if (allAtSameTimestamp) {
            Multimap<Cell, byte[]> cellValuesWithStrippedTimestamp = Multimaps.transformValues(cellValues, Value.GET_VALUE);

            Map<Cell, byte[]> putMap = Maps.transformValues(cellValuesWithStrippedTimestamp.asMap(), new Function<Collection<byte[]>, byte[]>() {

                @Override
                public byte[] apply(Collection<byte[]> input) {
                    try {
                        return Iterables.getOnlyElement(input);
                    } catch (IllegalArgumentException e) {
                        log.error("Application tried to put multiple same-cell values in at same timestamp; attempting to perform last-write-wins, but ordering is not guaranteed.");
                        return Iterables.getLast(input);
                    }
                }

            });

            put(tableRef, putMap, lastTimestamp);
            return;
        }
        delegate.putWithTimestamps(tableRef, cellValues);
    }

    @Override
    public void truncateTables(Set<TableReference> tableRefs) {
        if (tableRefs.isEmpty()) {
            return;
        }
        if (tableRefs.size() == 1) {
            truncateTable(Iterables.getOnlyElement(tableRefs));
            return;
        }
        delegate.truncateTables(tableRefs);
    }

    @Override
    public Map<byte[], RowColumnRangeIterator> getRowsColumnRange(TableReference tableRef, Iterable<byte[]> rows, ColumnRangeSelection columnRangeSelection, long timestamp) {
        return delegate().getRowsColumnRange(tableRef, rows, columnRangeSelection, timestamp);
    }
}
