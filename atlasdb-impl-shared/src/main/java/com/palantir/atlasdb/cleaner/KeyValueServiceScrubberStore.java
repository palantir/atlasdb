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
package com.palantir.atlasdb.cleaner;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.palantir.async.initializer.AsyncInitializer;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.common.base.AbstractBatchingVisitable;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.common.base.BatchingVisitableFromIterable;
import com.palantir.common.base.ClosableIterator;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;

/**
 *
 * A ScrubberStore implemented as a table in the KeyValueService.
 *
 */
public final class KeyValueServiceScrubberStore implements ScrubberStore {
    private class InitializingWrapper extends AsyncInitializer implements AutoDelegate_ScrubberStore {
        @Override
        public ScrubberStore delegate() {
            checkInitialized();
            return KeyValueServiceScrubberStore.this;
        }

        @Override
        protected void tryInitialize() {
            KeyValueServiceScrubberStore.this.tryInitialize();
        }

        @Override
        protected String getInitializingClassName() {
            return "KeyValueServiceScrubberStore";
        }
    }

    private static final byte[] EMPTY_CONTENTS = new byte[] {1};
    private final InitializingWrapper wrapper = new InitializingWrapper();
    private final KeyValueService keyValueService;

    public static ScrubberStore create(KeyValueService keyValueService) {
        return create(keyValueService, AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC);
    }

    public static ScrubberStore create(KeyValueService keyValueService, boolean initializeAsync) {
        KeyValueServiceScrubberStore scrubberStore = new KeyValueServiceScrubberStore(keyValueService);
        scrubberStore.wrapper.initialize(initializeAsync);
        return scrubberStore.wrapper.isInitialized() ? scrubberStore : scrubberStore.wrapper;
    }

    private KeyValueServiceScrubberStore(KeyValueService keyValueService) {
        this.keyValueService = keyValueService;
    }

    private void tryInitialize() {
        TableMetadata scrubTableMeta = TableMetadata.internal()
                .singleRowComponent("row", ValueType.BLOB)
                .dynamicColumns(ImmutableList.of(
                        NameComponentDescription.of("table", ValueType.VAR_STRING),
                        NameComponentDescription.of("col", ValueType.BLOB)),
                        ValueType.VAR_LONG)
                .build();
        keyValueService.createTable(AtlasDbConstants.SCRUB_TABLE, scrubTableMeta.persistToBytes());
    }

    @Override
    public boolean isInitialized() {
        return wrapper.isInitialized();
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
