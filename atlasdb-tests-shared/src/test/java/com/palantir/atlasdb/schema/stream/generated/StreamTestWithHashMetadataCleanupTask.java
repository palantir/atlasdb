package com.palantir.atlasdb.schema.stream.generated;

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

public class StreamTestWithHashMetadataCleanupTask implements OnCleanupTask {

    private final StreamTestTableFactory tables;

    public StreamTestWithHashMetadataCleanupTask(Namespace namespace) {
        tables = StreamTestTableFactory.of(namespace);
    }

    @Override
    public boolean cellsCleanedUp(Transaction t, Set<Cell> cells) {
        StreamTestWithHashStreamMetadataTable metaTable = tables.getStreamTestWithHashStreamMetadataTable(t);
        Set<StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow> rows = Sets.newHashSetWithExpectedSize(cells.size());
        for (Cell cell : cells) {
            rows.add(StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow.BYTES_HYDRATOR.hydrateFromBytes(cell.getRowName()));
        }
        StreamTestWithHashStreamIdxTable indexTable = tables.getStreamTestWithHashStreamIdxTable(t);
        Set<StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxRow> indexRows = rows.stream()
                .map(StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow::getId)
                .map(StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxRow::of)
                .collect(Collectors.toSet());
        Map<StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxRow, Iterator<StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxColumnValue>> indexIterator
                = indexTable.getRowsColumnRangeIterator(indexRows,
                        BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1));
        Set<StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow> rowsWithNoIndexEntries
                = KeyedStream.stream(indexIterator)
                .filter(valueIterator -> !valueIterator.hasNext())
                .keys()
                .map(StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxRow::getId)
                .map(StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow::of)
                .collect(Collectors.toSet());
        Map<StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow, StreamMetadata> currentMetadata = metaTable.getMetadatas(
                Sets.difference(rows, rowsWithNoIndexEntries));
        Set<Long> toDelete = Sets.newHashSet(rowsWithNoIndexEntries.stream()
                .map(StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow::getId)
                .collect(Collectors.toSet()));
        for (Map.Entry<StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow, StreamMetadata> e : currentMetadata.entrySet()) {
            if (e.getValue().getStatus() != Status.STORED) {
                toDelete.add(e.getKey().getId());
            }
        }
        StreamTestWithHashStreamStore.of(tables).deleteStreams(t, toDelete);
        return false;
    }
}