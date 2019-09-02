package com.palantir.atlasdb.performance.schema.generated;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import com.palantir.atlasdb.cleaner.api.OnCleanupTask;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.protos.generated.StreamPersistence.Status;
import com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.common.streams.KeyedStream;

public class ValueMetadataCleanupTask implements OnCleanupTask {

    private final StreamTestTableFactory tables;

    public ValueMetadataCleanupTask(Namespace namespace) {
        tables = StreamTestTableFactory.of(namespace);
    }

    @Override
    public boolean cellsCleanedUp(Transaction t, Set<Cell> cells) {
        ValueStreamMetadataTable metaTable = tables.getValueStreamMetadataTable(t);
        Set<ValueStreamMetadataTable.ValueStreamMetadataRow> rows = Sets.newHashSetWithExpectedSize(cells.size());
        for (Cell cell : cells) {
            rows.add(ValueStreamMetadataTable.ValueStreamMetadataRow.BYTES_HYDRATOR.hydrateFromBytes(cell.getRowName()));
        }
        ValueStreamIdxTable indexTable = tables.getValueStreamIdxTable(t);
        Set<ValueStreamIdxTable.ValueStreamIdxRow> indexRows = rows.stream()
                .map(ValueStreamMetadataTable.ValueStreamMetadataRow::getId)
                .map(ValueStreamIdxTable.ValueStreamIdxRow::of)
                .collect(Collectors.toSet());
        Map<ValueStreamIdxTable.ValueStreamIdxRow, Iterator<ValueStreamIdxTable.ValueStreamIdxColumnValue>> indexIterator
                = indexTable.getRowsColumnRangeIterator(indexRows,
                        BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1));
        Set<ValueStreamMetadataTable.ValueStreamMetadataRow> rowsWithNoIndexEntries
                = KeyedStream.stream(indexIterator)
                .filter(valueIterator -> !valueIterator.hasNext())
                .keys()
                .map(ValueStreamIdxTable.ValueStreamIdxRow::getId)
                .map(ValueStreamMetadataTable.ValueStreamMetadataRow::of)
                .collect(Collectors.toSet());
        Map<ValueStreamMetadataTable.ValueStreamMetadataRow, StreamMetadata> currentMetadata = metaTable.getMetadatas(
                Sets.difference(rows, rowsWithNoIndexEntries));
        Set<Long> toDelete = Sets.newHashSet(rowsWithNoIndexEntries.stream()
                .map(ValueStreamMetadataTable.ValueStreamMetadataRow::getId)
                .collect(Collectors.toSet()));
        for (Map.Entry<ValueStreamMetadataTable.ValueStreamMetadataRow, StreamMetadata> e : currentMetadata.entrySet()) {
            if (e.getValue().getStatus() != Status.STORED) {
                toDelete.add(e.getKey().getId());
            }
        }
        ValueStreamStore.of(tables).deleteStreams(t, toDelete);
        return false;
    }
}