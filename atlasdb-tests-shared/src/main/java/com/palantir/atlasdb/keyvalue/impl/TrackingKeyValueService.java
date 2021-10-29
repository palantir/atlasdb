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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.base.ClosableIterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TrackingKeyValueService extends ForwardingKeyValueService {
    private final Set<TableReference> tablesWrittenTo = Sets.newSetFromMap(new ConcurrentHashMap<>());
    private final Set<TableReference> tablesReadFrom = Sets.newSetFromMap(new ConcurrentHashMap<>());
    private final KeyValueService delegate;

    public TrackingKeyValueService(KeyValueService delegate) {
        this.delegate = delegate;
    }

    @Override
    public KeyValueService delegate() {
        return delegate;
    }

    @Override
    public Map<Cell, Value> get(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        tablesReadFrom.add(tableRef);
        return super.get(tableRef, timestampByCell);
    }

    @Override
    public Map<Cell, Value> getRows(
            TableReference tableRef, Iterable<byte[]> rows, ColumnSelection columnSelection, long timestamp) {
        tablesReadFrom.add(tableRef);
        return super.getRows(tableRef, rows, columnSelection, timestamp);
    }

    @Override
    public ClosableIterator<RowResult<Value>> getRange(
            TableReference tableRef, RangeRequest rangeRequest, long timestamp) {
        tablesReadFrom.add(tableRef);
        return super.getRange(tableRef, rangeRequest, timestamp);
    }

    @Override
    public void put(TableReference tableRef, Map<Cell, byte[]> values, long timestamp) {
        tablesWrittenTo.add(tableRef);
        super.put(tableRef, values, timestamp);
    }

    @Override
    public void multiPut(Map<TableReference, ? extends Map<Cell, byte[]>> valuesByTable, long timestamp) {
        tablesWrittenTo.addAll(valuesByTable.keySet());
        super.multiPut(valuesByTable, timestamp);
    }

    @Override
    public void putWithTimestamps(TableReference tableRef, Multimap<Cell, Value> values) {
        tablesWrittenTo.add(tableRef);
        super.putWithTimestamps(tableRef, values);
    }

    @Override
    public void putUnlessExists(TableReference tableRef, Map<Cell, byte[]> values) throws KeyAlreadyExistsException {
        tablesWrittenTo.add(tableRef);
        super.putUnlessExists(tableRef, values);
    }

    @Override
    public void putToCasTable(TableReference tableRef, Map<Cell, byte[]> values) {
        tablesWrittenTo.add(tableRef);
        super.putToCasTable(tableRef, values);
    }

    @Override
    public void checkAndSet(CheckAndSetRequest request) {
        tablesWrittenTo.add(request.table());
        super.checkAndSet(request);
    }

    public Set<TableReference> getTablesWrittenTo() {
        return ImmutableSet.copyOf(tablesWrittenTo);
    }

    public Set<TableReference> getTablesReadFrom() {
        return ImmutableSet.copyOf(tablesReadFrom);
    }

    public void clearTablesWrittenTo() {
        tablesWrittenTo.clear();
    }

    public void clearTablesReadFrom() {
        tablesReadFrom.clear();
    }
}
