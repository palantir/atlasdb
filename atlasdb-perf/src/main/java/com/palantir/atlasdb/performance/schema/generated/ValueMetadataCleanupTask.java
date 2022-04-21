package com.palantir.atlasdb.performance.schema.generated;

import java.util.Iterator;
import java.util.Map;
import java.util.HashSet;
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
import com.palantir.atlasdb.transaction.api.Transaction;

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
        Set<ValueStreamMetadataTable.ValueStreamMetadataRow> rowsWithNoIndexEntries =
                        getUnreferencedStreamsByIterator(indexTable, rows);
        Set<Long> toDelete = new HashSet<>();
        Map<ValueStreamMetadataTable.ValueStreamMetadataRow, StreamMetadata> currentMetadata =
                metaTable.getMetadatas(rows);
        for (Map.Entry<ValueStreamMetadataTable.ValueStreamMetadataRow, StreamMetadata> e : currentMetadata.entrySet()) {
            if (e.getValue().getStatus() != Status.STORED || rowsWithNoIndexEntries.contains(e.getKey())) {
                toDelete.add(e.getKey().getId());
            }
        }
        ValueStreamStore.of(tables).deleteStreams(t, toDelete);
        return false;
    }

    private static Set<ValueStreamMetadataTable.ValueStreamMetadataRow> getUnreferencedStreamsByIterator(ValueStreamIdxTable indexTable, Set<ValueStreamMetadataTable.ValueStreamMetadataRow> metadataRows) {
        Set<ValueStreamIdxTable.ValueStreamIdxRow> indexRows = metadataRows.stream()
                .map(ValueStreamMetadataTable.ValueStreamMetadataRow::getId)
                .map(ValueStreamIdxTable.ValueStreamIdxRow::of)
                .collect(Collectors.toSet());
        Map<ValueStreamIdxTable.ValueStreamIdxRow, Iterator<ValueStreamIdxTable.ValueStreamIdxColumnValue>> referenceIteratorByStream
                = indexTable.getRowsColumnRangeIterator(indexRows,
                        BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1));
        return referenceIteratorByStream.entrySet().stream()
                .filter(entry -> !entry.getValue().hasNext())
                .map(Map.Entry::getKey)
                .map(ValueStreamIdxTable.ValueStreamIdxRow::getId)
                .map(ValueStreamMetadataTable.ValueStreamMetadataRow::of)
                .collect(Collectors.toSet());
    }
}