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

package com.palantir.atlasdb.sweep.queue.clear;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.schema.generated.TableClearsTable.TableClearsNamedColumn;
import com.palantir.atlasdb.schema.generated.TableClearsTable.TableClearsRow;
import com.palantir.atlasdb.schema.generated.TableClearsTable.TableClearsRowResult;
import com.palantir.atlasdb.schema.generated.TargetedSweepTableFactory;
import com.palantir.common.streams.KeyedStream;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public final class DefaultConservativeSweepWatermarkStore implements ConservativeSweepWatermarkStore {
    private static final TableReference CLEARS = TargetedSweepTableFactory.of().getTableClearsTable(null).getTableRef();
    private final KeyValueService kvs;

    public DefaultConservativeSweepWatermarkStore(KeyValueService kvs) {
        this.kvs = kvs;
    }

    @Override
    public void updateWatermarks(long newWatermark, Set<TableReference> conservativeTables) {
        conservativeTables.forEach(table -> updateWatermarkForTable(newWatermark, table));
    }

    private void updateWatermarkForTable(long newWatermark, TableReference table) {
        while (true) {
            Cell cell = cell(table);
            Optional<Value> currentValue = Optional.ofNullable(
                    kvs.get(CLEARS, ImmutableMap.of(cell, Long.MAX_VALUE)).get(cell));
            Optional<Long> currentWatermark = currentValue.map(value -> RowResult.of(cell, value.getContents()))
                    .map(TableClearsRowResult::of)
                    .map(TableClearsRowResult::getLastClearedTimestamp);

            if (currentWatermark.isPresent() && currentWatermark.get() > newWatermark) {
                return;
            }

            CheckAndSetRequest request = currentWatermark
                    .map(watermark -> CheckAndSetRequest.singleCell(
                            CLEARS,
                            cell,
                            EncodingUtils.encodeVarLong(watermark),
                            EncodingUtils.encodeVarLong(newWatermark)))
                    .orElseGet(() -> CheckAndSetRequest.newCell(
                            CLEARS, cell, EncodingUtils.encodeVarLong(newWatermark)));

            try {
                kvs.checkAndSet(request);
                return;
            } catch (CheckAndSetException e) {
                // do nothing, we spin
            }
        }
    }

    private static Cell cell(TableReference table) {
        TableClearsRow row = TableClearsRow.of(table.getQualifiedName());
        return Cell.create(row.persistToBytes(), TableClearsNamedColumn.LAST_CLEARED_TIMESTAMP.getShortName());
    }


    @Override
    public Map<TableReference, Long> getWatermarks(Set<TableReference> tableReferences) {
        Set<Cell> cells = tableReferences.stream().map(table -> cell(table)).collect(Collectors.toSet());
        Map<Cell, Value> fetched = kvs.get(CLEARS, Maps.asMap(cells, ignored -> Long.MAX_VALUE));
        return KeyedStream.stream(fetched)
                .map((cell, value) -> RowResult.of(cell, value.getContents()))
                .map(TableClearsRowResult::of)
                .mapKeys((cell, rowResult) -> tableRef(rowResult.getRowName()))
                .map(TableClearsRowResult::getLastClearedTimestamp)
                .collectToMap();
    }

    private static TableReference tableRef(TableClearsRow row) {
        return TableReference.fromString(row.getTable());
    }
}
