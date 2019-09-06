package com.palantir.atlasdb.blob.generated;

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

public class DataMetadataCleanupTask implements OnCleanupTask {

    private final BlobSchemaTableFactory tables;

    public DataMetadataCleanupTask(Namespace namespace) {
        tables = BlobSchemaTableFactory.of(namespace);
    }

    @Override
    public boolean cellsCleanedUp(Transaction t, Set<Cell> cells) {
        DataStreamMetadataTable metaTable = tables.getDataStreamMetadataTable(t);
        Set<DataStreamMetadataTable.DataStreamMetadataRow> rows = Sets.newHashSetWithExpectedSize(cells.size());
        for (Cell cell : cells) {
            rows.add(DataStreamMetadataTable.DataStreamMetadataRow.BYTES_HYDRATOR.hydrateFromBytes(cell.getRowName()));
        }
        DataStreamIdxTable indexTable = tables.getDataStreamIdxTable(t);
        Set<DataStreamIdxTable.DataStreamIdxRow> indexRows = rows.stream()
                .map(DataStreamMetadataTable.DataStreamMetadataRow::getId)
                .map(DataStreamIdxTable.DataStreamIdxRow::of)
                .collect(Collectors.toSet());
        Map<DataStreamIdxTable.DataStreamIdxRow, Iterator<DataStreamIdxTable.DataStreamIdxColumnValue>> referenceIteratorByStream
                = indexTable.getRowsColumnRangeIterator(indexRows,
                        BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1));
        Set<DataStreamMetadataTable.DataStreamMetadataRow> streamsWithNoReferences
                = KeyedStream.stream(referenceIteratorByStream)
                .filter(valueIterator -> !valueIterator.hasNext())
                .keys()
                .map(DataStreamIdxTable.DataStreamIdxRow::getId)
                .map(DataStreamMetadataTable.DataStreamMetadataRow::of)
                .collect(Collectors.toSet());
        Map<DataStreamMetadataTable.DataStreamMetadataRow, StreamMetadata> currentMetadata = metaTable.getMetadatas(rows);
        Set<Long> toDelete = Sets.newHashSet();
        for (Map.Entry<DataStreamMetadataTable.DataStreamMetadataRow, StreamMetadata> e : currentMetadata.entrySet()) {
            if (e.getValue().getStatus() != Status.STORED || streamsWithNoReferences.contains(e.getKey())) {
                toDelete.add(e.getKey().getId());
            }
        }
        DataStreamStore.of(tables).deleteStreams(t, toDelete);
        return false;
    }
}