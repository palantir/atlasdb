/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.queue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.keyvalue.api.ImmutableTargetedSweepMetadata;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.TableReferenceAndCell;
import com.palantir.atlasdb.keyvalue.api.TargetedSweepMetadata;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.schema.generated.SweepableCellsTable;
import com.palantir.atlasdb.schema.generated.TargetedSweepTableFactory;
import com.palantir.common.base.ClosableIterator;

public class SweepableCellsReader {
    private static final TableReference TABLE_REF = TargetedSweepTableFactory.of()
            .getSweepableCellsTable(null).getTableRef();
    private final KeyValueService kvs;

    SweepableCellsReader(KeyValueService kvs) {
        this.kvs = kvs;
    }

    List<WriteInfo> getTombstonesToWrite(long tsPartition, int shard, boolean conservative) {
        TargetedSweepMetadata metadata = ImmutableTargetedSweepMetadata.builder()
                .conservative(conservative)
                .dedicatedRow(false)
                .shard(shard)
                .dedicatedRowNumber(0)
                .build();

        SweepableCellsTable.SweepableCellsRow row = SweepableCellsTable.SweepableCellsRow.of(
                tsPartition, metadata.persistToBytes());
        RangeRequest request = SweepQueueUtils.requestColsForRow(row.persistToBytes());

        ClosableIterator<RowResult<Value>> rowResultIterator = kvs.getRange(TABLE_REF, request, SweepQueueUtils.CAS_TS);

        if (!rowResultIterator.hasNext()) {
            return ImmutableList.of();
        }

        RowResult<Value> rowResult = rowResultIterator.next();

        Map<TableReferenceAndCell, Long> list = new HashMap<>();

        rowResult.getColumns().entrySet().forEach(entry -> add(row, entry, list));
        return list.entrySet().stream()
                .map(entry -> WriteInfo.of(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
    }

    private void add(SweepableCellsTable.SweepableCellsRow row,
            Map.Entry<byte[], Value> entry, Map<TableReferenceAndCell, Long> list) {
        SweepableCellsTable.SweepableCellsColumn col = SweepableCellsTable.SweepableCellsColumn.BYTES_HYDRATOR
                .hydrateFromBytes(entry.getKey());
        if (isReferenceToDedicatedRows(col)) {
            addDedicated(row, col, list);
        }
        addNonDedicated(row, col, entry.getValue(), list);
    }

    private void addNonDedicated(SweepableCellsTable.SweepableCellsRow row,
            SweepableCellsTable.SweepableCellsColumn col, Value value, Map<TableReferenceAndCell, Long> map) {
        TableReferenceAndCell tableRefCell = SweepableCellsTable.SweepableCellsColumnValue
                .hydrateValue(value.getContents());
        long timestamp = row.getTimestampPartition() * SweepQueueUtils.TS_FINE_GRANULARITY + col.getTimestampModulus();
        map.merge(tableRefCell, timestamp, Math::max);
    }

    private void addDedicated(SweepableCellsTable.SweepableCellsRow row,
            SweepableCellsTable.SweepableCellsColumn col, Map<TableReferenceAndCell, Long> list) {
        TargetedSweepMetadata metadata = TargetedSweepMetadata.BYTES_HYDRATOR.hydrateFromBytes(row.getMetadata());
        int numberOfDedicatedRows = (int) -col.getWriteIndex();
        TargetedSweepMetadata startMetadata = ImmutableTargetedSweepMetadata.builder()
                .from(metadata)
                .dedicatedRow(true)
                .build();
        TargetedSweepMetadata endMetadata = ImmutableTargetedSweepMetadata.builder()
                .from(startMetadata)
                .dedicatedRowNumber(numberOfDedicatedRows - 1)
                .build();
        byte[] startRow = SweepableCellsTable.SweepableCellsRow.of(row.getTimestampPartition(),
                startMetadata.persistToBytes()).persistToBytes();
        byte[] endRow = SweepableCellsTable.SweepableCellsRow.of(row.getTimestampPartition(),
                endMetadata.persistToBytes()).persistToBytes();
        RangeRequest request = SweepQueueUtils.requestColsForRowRange(startRow, endRow, numberOfDedicatedRows);
        ClosableIterator<RowResult<Value>> rowResultIterator = kvs.getRange(TABLE_REF, request, SweepQueueUtils.CAS_TS);
        rowResultIterator.forEachRemaining(rowResult -> addFromRowResult(row, rowResult, list));
    }

    private void addFromRowResult(SweepableCellsTable.SweepableCellsRow row, RowResult<Value> rowResult,
            Map<TableReferenceAndCell, Long> list) {
        rowResult.getColumns().entrySet().forEach(valueEntry -> add(row, valueEntry, list));
    }

    private boolean isReferenceToDedicatedRows(SweepableCellsTable.SweepableCellsColumn col) {
        return col.getWriteIndex() < 0;
    }
}
