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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ImmutableTargetedSweepMetadata;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.TableReferenceAndCell;
import com.palantir.atlasdb.keyvalue.api.TargetedSweepMetadata;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.schema.generated.SweepableCellsTable;
import com.palantir.atlasdb.schema.generated.TargetedSweepTableFactory;

public class SweepableCellsReader {
    private static final TableReference TABLE_REF = TargetedSweepTableFactory.of()
            .getSweepableCellsTable(null).getTableRef();
    private static final ColumnRangeSelection ALL_COLUMNS = allPossibleColumns();

    private final KeyValueService kvs;

    SweepableCellsReader(KeyValueService kvs) {
        this.kvs = kvs;
    }

    List<WriteInfo> getLatestWrites(long partitionFine, ShardAndStrategy shardAndStrategy) {
        TargetedSweepMetadata metadata = getDefaultMetadata(shardAndStrategy);
        SweepableCellsTable.SweepableCellsRow row = SweepableCellsTable.SweepableCellsRow.of(
                partitionFine, metadata.persistToBytes());

        RowColumnRangeIterator resultIterator = getAllColumns(row);

        Map<TableReferenceAndCell, Long> results = new HashMap<>();
        resultIterator.forEachRemaining(entry -> populateResults(row, entry, results));

        return results.entrySet().stream()
                .map(entry -> WriteInfo.of(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
    }

    private TargetedSweepMetadata getDefaultMetadata(ShardAndStrategy shardAndStrategy) {
        return ImmutableTargetedSweepMetadata.builder()
                .conservative(shardAndStrategy.isConservative())
                .dedicatedRow(false)
                .shard(shardAndStrategy.shard())
                .dedicatedRowNumber(0)
                .build();
    }

    private RowColumnRangeIterator getAllColumns(SweepableCellsTable.SweepableCellsRow row) {
        return getAllColumns(ImmutableList.of(row.persistToBytes()));
    }

    private RowColumnRangeIterator getAllColumns(Iterable<byte[]> rows) {
        return kvs.getRowsColumnRange(TABLE_REF, rows, ALL_COLUMNS, 1000, SweepQueueUtils.CAS_TS);
    }

    private void populateResults(SweepableCellsTable.SweepableCellsRow row, Map.Entry<Cell, Value> entry,
            Map<TableReferenceAndCell, Long> results) {
        SweepableCellsTable.SweepableCellsColumn col = SweepableCellsTable.SweepableCellsColumn.BYTES_HYDRATOR
                .hydrateFromBytes(entry.getKey().getColumnName());

        if (isReferenceToDedicatedRows(col)) {
            populateFromDedicatedRows(row, col, results);
        } else {
            populateFromValue(row, col, entry.getValue(), results);
        }
    }

    private boolean isReferenceToDedicatedRows(SweepableCellsTable.SweepableCellsColumn col) {
        return col.getWriteIndex() < 0;
    }

    private void populateFromDedicatedRows(SweepableCellsTable.SweepableCellsRow row,
            SweepableCellsTable.SweepableCellsColumn col, Map<TableReferenceAndCell, Long> results) {
        TargetedSweepMetadata metadata = TargetedSweepMetadata.BYTES_HYDRATOR.hydrateFromBytes(row.getMetadata());
        long partitionFine = row.getTimestampPartition();
        int numberOfDedicatedRows = (int) -col.getWriteIndex();

        List<byte[]> dedicatedRows = new ArrayList<>();
        for (int i = 0; i < numberOfDedicatedRows; i++) {
            byte[] metadataBytes = ImmutableTargetedSweepMetadata.builder()
                    .from(metadata)
                    .dedicatedRow(true)
                    .dedicatedRowNumber(i)
                    .build()
                    .persistToBytes();
            dedicatedRows.add(SweepableCellsTable.SweepableCellsRow.of(partitionFine, metadataBytes).persistToBytes());
        }
        RowColumnRangeIterator iterator = getAllColumns(dedicatedRows);
        iterator.forEachRemaining(entry -> populateResults(row, entry, results));
    }

    private void populateFromValue(SweepableCellsTable.SweepableCellsRow row,
            SweepableCellsTable.SweepableCellsColumn col,
            Value value,
            Map<TableReferenceAndCell, Long> results) {
        TableReferenceAndCell tableRefCell = SweepableCellsTable.SweepableCellsColumnValue
                .hydrateValue(value.getContents());

        long timestamp = row.getTimestampPartition() * SweepQueueUtils.TS_FINE_GRANULARITY + col.getTimestampModulus();
        addIfMaxForCell(timestamp, tableRefCell, results);
    }

    private void addIfMaxForCell(long ts, TableReferenceAndCell refAndCell, Map<TableReferenceAndCell, Long> result) {
        result.merge(refAndCell, ts, Math::max);
    }

    private static ColumnRangeSelection allPossibleColumns() {
        byte[] startCol = SweepableCellsTable.SweepableCellsColumn.of(0L, -TargetedSweepMetadata.MAX_DEDICATED_ROWS)
                .persistToBytes();
        byte[] endCol = SweepableCellsTable.SweepableCellsColumn.of(SweepQueueUtils.TS_FINE_GRANULARITY + 1, 0)
                .persistToBytes();
        return new ColumnRangeSelection(startCol, endCol);
    }
}
