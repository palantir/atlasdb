package com.palantir.atlasdb.schema.stream.generated;

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

public class StreamTestMetadataCleanupTask implements OnCleanupTask {

    private final StreamTestTableFactory tables;

    public StreamTestMetadataCleanupTask(Namespace namespace) {
        tables = StreamTestTableFactory.of(namespace);
    }

    @Override
    public boolean cellsCleanedUp(Transaction t, Set<Cell> cells) {
        StreamTestStreamMetadataTable metaTable = tables.getStreamTestStreamMetadataTable(t);
        Set<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow> rows = Sets.newHashSetWithExpectedSize(cells.size());
        for (Cell cell : cells) {
            rows.add(StreamTestStreamMetadataTable.StreamTestStreamMetadataRow.BYTES_HYDRATOR.hydrateFromBytes(cell.getRowName()));
        }
        StreamTestStreamIdxTable indexTable = tables.getStreamTestStreamIdxTable(t);
        Set<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow> rowsWithNoIndexEntries =
                        getUnreferencedStreamsByIterator(indexTable, rows);
        Set<Long> toDelete = new HashSet<>();
        Map<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow, StreamMetadata> currentMetadata =
                metaTable.getMetadatas(rows);
        for (Map.Entry<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow, StreamMetadata> e : currentMetadata.entrySet()) {
            if (e.getValue().getStatus() != Status.STORED || rowsWithNoIndexEntries.contains(e.getKey())) {
                toDelete.add(e.getKey().getId());
            }
        }
        StreamTestStreamStore.of(tables).deleteStreams(t, toDelete);
        return false;
    }

    private static Set<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow> getUnreferencedStreamsByIterator(StreamTestStreamIdxTable indexTable, Set<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow> metadataRows) {
        Set<StreamTestStreamIdxTable.StreamTestStreamIdxRow> indexRows = metadataRows.stream()
                .map(StreamTestStreamMetadataTable.StreamTestStreamMetadataRow::getId)
                .map(StreamTestStreamIdxTable.StreamTestStreamIdxRow::of)
                .collect(Collectors.toSet());
        Map<StreamTestStreamIdxTable.StreamTestStreamIdxRow, Iterator<StreamTestStreamIdxTable.StreamTestStreamIdxColumnValue>> referenceIteratorByStream
                = indexTable.getRowsColumnRangeIterator(indexRows,
                        BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1));
        return referenceIteratorByStream.entrySet().stream()
                .filter(entry -> !entry.getValue().hasNext())
                .map(Map.Entry::getKey)
                .map(StreamTestStreamIdxTable.StreamTestStreamIdxRow::getId)
                .map(StreamTestStreamMetadataTable.StreamTestStreamMetadataRow::of)
                .collect(Collectors.toSet());
    }
}