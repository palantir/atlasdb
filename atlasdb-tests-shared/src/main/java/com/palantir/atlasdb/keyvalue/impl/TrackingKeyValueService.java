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

import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.base.ClosableIterator;

public class TrackingKeyValueService extends ForwardingKeyValueService {
    private final Set<String> tablesWrittenTo = Sets.newSetFromMap(Maps.<String, Boolean>newConcurrentMap());
    private final Set<String> tablesReadFrom = Sets.newSetFromMap(Maps.<String, Boolean>newConcurrentMap());
    private final KeyValueService delegate;

    public TrackingKeyValueService(KeyValueService delegate) {
        this.delegate = delegate;
    }

    @Override
    protected KeyValueService delegate() {
        return delegate;
    }

    @Override
    public Map<Cell, Value> get(String tableName, Map<Cell, Long> timestampByCell) {
        tablesReadFrom.add(tableName);
        return super.get(tableName, timestampByCell);
    }

    @Override
    public Map<Cell, Value> getRows(String tableName, Iterable<byte[]> rows,
                                    ColumnSelection columnSelection, long timestamp) {
        tablesReadFrom.add(tableName);
        return super.getRows(tableName, rows, columnSelection, timestamp);
    }

    @Override
    public ClosableIterator<RowResult<Value>> getRange(String tableName, RangeRequest rangeRequest, long timestamp) {
        tablesReadFrom.add(tableName);
        return super.getRange(tableName, rangeRequest, timestamp);
    }

    @Override
    public void put(String tableName, Map<Cell, byte[]> values, long timestamp) {
        tablesWrittenTo.add(tableName);
        super.put(tableName, values, timestamp);
    }

    @Override
    public void multiPut(Map<String, ? extends Map<Cell, byte[]>> valuesByTable, long timestamp) {
        tablesWrittenTo.addAll(valuesByTable.keySet());
        super.multiPut(valuesByTable, timestamp);
    }

    @Override
    public void putWithTimestamps(String tableName, Multimap<Cell, Value> values) {
        tablesWrittenTo.add(tableName);
        super.putWithTimestamps(tableName, values);
    }

    @Override
    public void putUnlessExists(String tableName, Map<Cell, byte[]> values) throws KeyAlreadyExistsException {
        tablesWrittenTo.add(tableName);
        super.putUnlessExists(tableName, values);
    }

    public Set<String> getTablesWrittenTo() {
        return ImmutableSet.copyOf(tablesWrittenTo);
    }

    public Set<String> getTablesReadFrom() {
        return ImmutableSet.copyOf(tablesReadFrom);
    }

    public void clearTablesWrittenTo() {
        tablesWrittenTo.clear();
    }

    public void clearTablesReadFrom() {
        tablesReadFrom.clear();
    }
}
