/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.cleaner;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.DynamicColumnDescription;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.common.base.AbstractBatchingVisitable;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.common.base.BatchingVisitableFromIterable;
import com.palantir.common.base.ClosableIterator;

/**
 *
 * A ScrubberStore implemented as a table in the KeyValueService.
 *
 *
 * @author ejin
 */
public final class KeyValueServiceScrubberStore implements ScrubberStore {
    private static final byte[] EMPTY_CONTENTS = new byte[] {1};
    private final KeyValueService keyValueService;

    public static ScrubberStore create(KeyValueService keyValueService) {
        TableMetadata scrubTableMeta = new TableMetadata(
                NameMetadataDescription.create(ImmutableList.of(
                        new NameComponentDescription.Builder()
                            .componentName("row")
                            .type(ValueType.BLOB)
                            .build())),
                new ColumnMetadataDescription(new DynamicColumnDescription(
                        NameMetadataDescription.create(ImmutableList.of(
                                new NameComponentDescription.Builder()
                                    .componentName("table")
                                    .type(ValueType.VAR_STRING)
                                    .build(),
                                new NameComponentDescription.Builder()
                                    .componentName("col")
                                    .type(ValueType.BLOB)
                                    .build())),
                        ColumnValueDescription.forType(ValueType.VAR_LONG))),
                ConflictHandler.IGNORE_ALL);
        keyValueService.createTable(AtlasDbConstants.SCRUB_TABLE, scrubTableMeta.persistToBytes());
        return new KeyValueServiceScrubberStore(keyValueService);
    }

    private KeyValueServiceScrubberStore(KeyValueService keyValueService) {
        this.keyValueService = keyValueService;
    }

    @Override
    public void queueCellsForScrubbing(
            Multimap<Cell, TableReference> cellToTableRefs,
            long scrubTimestamp,
            int batchSize) {
        Map<Cell, byte[]> values = Maps.newHashMap();
        for (Map.Entry<Cell, Collection<TableReference>> entry : cellToTableRefs.asMap().entrySet()) {
            Cell cell = entry.getKey();
            for (TableReference tableRef : entry.getValue()) {
                byte[] tableBytes = EncodingUtils.encodeVarString(tableRef.getQualifiedName());
                byte[] col = EncodingUtils.add(tableBytes, cell.getColumnName());
                values.put(Cell.create(cell.getRowName(), col), EMPTY_CONTENTS);
            }
        }
        for (List<Entry<Cell, byte[]>> batch : Iterables.partition(values.entrySet(), batchSize)) {
            Map<Cell, byte[]> batchMap = Maps.newHashMap();
            for (Entry<Cell, byte[]> e : batch) {
                batchMap.put(e.getKey(), e.getValue());
            }
            keyValueService.put(
                    AtlasDbConstants.SCRUB_TABLE,
                    batchMap,
                    scrubTimestamp);
        }
    }

    @Override
    public void markCellsAsScrubbed(Map<TableReference, Multimap<Cell, Long>> cellToScrubTimestamp, int batchSize) {
        Multimap<Cell, Long> batch = ArrayListMultimap.create();
        for (Entry<TableReference, Multimap<Cell, Long>> tableEntry : cellToScrubTimestamp.entrySet()) {
            byte[] tableBytes = EncodingUtils.encodeVarString(tableEntry.getKey().getQualifiedName());
            for (Entry<Cell, Collection<Long>> cellEntry : tableEntry.getValue().asMap().entrySet()) {
                byte[] col = EncodingUtils.add(tableBytes, cellEntry.getKey().getColumnName());
                Cell cell = Cell.create(cellEntry.getKey().getRowName(), col);
                for (Long timestamp : cellEntry.getValue()) {
                    batch.put(cell, timestamp);
                    if (batch.size() >= batchSize) {
                        keyValueService.delete(AtlasDbConstants.SCRUB_TABLE, batch);
                        batch.clear();
                    }
                }
            }
        }
        if (!batch.isEmpty()) {
            keyValueService.delete(AtlasDbConstants.SCRUB_TABLE, batch);
        }
    }

    @Override
    public BatchingVisitable<SortedMap<Long, Multimap<TableReference, Cell>>> getBatchingVisitableScrubQueue(
            long maxScrubTimestamp /* exclusive */,
            byte[] startRow,
            byte[] endRow) {
        return new AbstractBatchingVisitable<SortedMap<Long, Multimap<TableReference, Cell>>>() {
            @Override
            protected <K extends Exception> void batchAcceptSizeHint(
                    int batchSizeHint,
                    ConsistentVisitor<SortedMap<Long, Multimap<TableReference, Cell>>, K> visitor) throws K {
                ClosableIterator<RowResult<Value>> iterator =
                        getIteratorToScrub(batchSizeHint, maxScrubTimestamp, startRow, endRow);
                try {
                    BatchingVisitableFromIterable.create(iterator).batchAccept(
                            batchSizeHint, batch -> visitor.visitOne(transformRows(batch)));
                } finally {
                    iterator.close();
                }
            }
        };
    }

    private ClosableIterator<RowResult<Value>> getIteratorToScrub(
            int batchSizeHint,
            long maxScrubTimestamp,
            byte[] startRow,
            byte[] endRow) {
        RangeRequest.Builder range = RangeRequest.builder();
        if (startRow != null) {
            range = range.startRowInclusive(startRow);
        }
        if (endRow != null) {
            range = range.endRowExclusive(endRow);
        }
        return keyValueService.getRange(
                AtlasDbConstants.SCRUB_TABLE,
                range.batchHint(batchSizeHint).build(),
                maxScrubTimestamp);
    }

    private SortedMap<Long, Multimap<TableReference, Cell>> transformRows(List<RowResult<Value>> input) {
        SortedMap<Long, Multimap<TableReference, Cell>> scrubTimestampToTableNameToCell = Maps.newTreeMap();
        for (RowResult<Value> rowResult : input) {
            byte[] row = rowResult.getRowName();
            for (Entry<byte[], Value> entry : rowResult.getColumns().entrySet()) {
                byte[] fullCol = entry.getKey();
                String table = EncodingUtils.decodeVarString(fullCol);
                byte[] col = Arrays.copyOfRange(fullCol, EncodingUtils.sizeOfVarString(table), fullCol.length);
                TableReference tableRef = TableReference.fromString(table);
                Cell cell = Cell.create(row, col);
                long timestamp = entry.getValue().getTimestamp();
                Multimap<TableReference, Cell> cells = scrubTimestampToTableNameToCell.get(timestamp);
                if (cells == null) {
                    cells = ArrayListMultimap.create();
                    scrubTimestampToTableNameToCell.put(timestamp, cells);
                }
                cells.put(tableRef, cell);
            }
        }
        return scrubTimestampToTableNameToCell;
    }

    @Override
    public int getNumberRemainingScrubCells(int maxCellsToScan) {
        try (ClosableIterator<RowResult<Value>> iterator =
                getIteratorToScrub(maxCellsToScan, Long.MAX_VALUE, null, null)) {
            return Iterators.size(Iterators.limit(iterator, maxCellsToScan));
        }
    }
}
