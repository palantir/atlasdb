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
package com.palantir.atlasdb.sweep.queue.id;

import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.schema.generated.SweepIdToNameTable.SweepIdToNameColumn;
import com.palantir.atlasdb.schema.generated.SweepIdToNameTable.SweepIdToNameColumnValue;
import com.palantir.atlasdb.schema.generated.SweepIdToNameTable.SweepIdToNameRow;
import com.palantir.atlasdb.schema.generated.SweepIdToNameTable.SweepIdToNameRowResult;
import com.palantir.atlasdb.schema.generated.TargetedSweepTableFactory;
import java.util.Collections;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

class IdsToNames {
    private static final TargetedSweepTableFactory tableFactory = TargetedSweepTableFactory.of();
    private static final TableReference ID_TO_NAME = tableFactory.getSweepIdToNameTable(null).getTableRef();
    private static final byte[] rowAsBytes = SweepIdToNameRow.of("s").persistToBytes();

    private final KeyValueService kvs;

    IdsToNames(KeyValueService kvs) {
        this.kvs = kvs;
    }

    boolean storeNewMapping(TableReference table, int id) {
        SweepIdToNameColumn column = SweepIdToNameColumn.of(id);
        SweepIdToNameColumnValue value = SweepIdToNameColumnValue.of(column, table.getQualifiedName());
        Cell cell = Cell.create(rowAsBytes, value.persistColumnName());
        CheckAndSetRequest request = CheckAndSetRequest.newCell(ID_TO_NAME, cell, value.persistValue());
        try {
            kvs.checkAndSet(request);
            return true;
        } catch (CheckAndSetException e) {
            return get(id).get().equals(table);
        }
    }

    Optional<TableReference> get(int tableId) {
        SweepIdToNameColumn column = SweepIdToNameColumn.of(tableId);
        Cell cell = Cell.create(rowAsBytes, column.persistToBytes());
        Map<Cell, Value> values = kvs.get(ID_TO_NAME, Collections.singletonMap(cell, Long.MAX_VALUE));
        return Optional.ofNullable(values.get(cell))
                .map(Value::getContents)
                .map(SweepIdToNameColumnValue::hydrateValue)
                .map(TableReference::fromString);
    }

    int getNextId() {
        RowColumnRangeIterator iterator = kvs.getRowsColumnRange(
                ID_TO_NAME, Collections.singleton(rowAsBytes),
                BatchColumnRangeSelection.create(null, null, 1),
                Long.MAX_VALUE).getOrDefault(rowAsBytes, emptyIterator());
        if (!iterator.hasNext()) {
            return 1;
        }
        Map.Entry<Cell, Value> first = iterator.next();
        RowResult<byte[]> rowResult = RowResult.of(first.getKey(), first.getValue().getContents());
        SweepIdToNameRowResult deserializedRowResult = SweepIdToNameRowResult.of(rowResult);
        return Ints.checkedCast(
                Iterables.getOnlyElement(deserializedRowResult.getColumnValues()).getColumnName().getTableId()) + 1;
    }

    private static RowColumnRangeIterator emptyIterator() {
        return new RowColumnRangeIterator() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public Map.Entry<Cell, Value> next() {
                throw new NoSuchElementException();
            }
        };
    }
}
