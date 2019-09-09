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

public class StreamTestMaxMemMetadataCleanupTask implements OnCleanupTask {

    private final StreamTestTableFactory tables;

    public StreamTestMaxMemMetadataCleanupTask(Namespace namespace) {
        tables = StreamTestTableFactory.of(namespace);
    }

    @Override
    public boolean cellsCleanedUp(Transaction t, Set<Cell> cells) {
        StreamTestMaxMemStreamMetadataTable metaTable = tables.getStreamTestMaxMemStreamMetadataTable(t);
        Set<StreamTestMaxMemStreamMetadataTable.StreamTestMaxMemStreamMetadataRow> rows = Sets.newHashSetWithExpectedSize(cells.size());
        for (Cell cell : cells) {
            rows.add(StreamTestMaxMemStreamMetadataTable.StreamTestMaxMemStreamMetadataRow.BYTES_HYDRATOR.hydrateFromBytes(cell.getRowName()));
        }
        StreamTestMaxMemStreamIdxTable indexTable = tables.getStreamTestMaxMemStreamIdxTable(t);
        Set<StreamTestMaxMemStreamIdxTable.StreamTestMaxMemStreamIdxRow> indexRows = rows.stream()
                .map(StreamTestMaxMemStreamMetadataTable.StreamTestMaxMemStreamMetadataRow::getId)
                .map(StreamTestMaxMemStreamIdxTable.StreamTestMaxMemStreamIdxRow::of)
                .collect(Collectors.toSet());
        Map<StreamTestMaxMemStreamIdxTable.StreamTestMaxMemStreamIdxRow, Iterator<StreamTestMaxMemStreamIdxTable.StreamTestMaxMemStreamIdxColumnValue>> referenceIteratorByStream
                = indexTable.getRowsColumnRangeIterator(indexRows,
                        BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1));
        Set<StreamTestMaxMemStreamMetadataTable.StreamTestMaxMemStreamMetadataRow> streamsWithNoReferences
                = KeyedStream.stream(referenceIteratorByStream)
                .filter(valueIterator -> !valueIterator.hasNext())
                .keys() // (authorized)
                .map(StreamTestMaxMemStreamIdxTable.StreamTestMaxMemStreamIdxRow::getId)
                .map(StreamTestMaxMemStreamMetadataTable.StreamTestMaxMemStreamMetadataRow::of)
                .collect(Collectors.toSet());
        Map<StreamTestMaxMemStreamMetadataTable.StreamTestMaxMemStreamMetadataRow, StreamMetadata> currentMetadata = metaTable.getMetadatas(rows);
        Set<Long> toDelete = Sets.newHashSet();
        for (Map.Entry<StreamTestMaxMemStreamMetadataTable.StreamTestMaxMemStreamMetadataRow, StreamMetadata> e : currentMetadata.entrySet()) {
            if (e.getValue().getStatus() != Status.STORED || streamsWithNoReferences.contains(e.getKey())) {
                toDelete.add(e.getKey().getId());
            }
        }
        StreamTestMaxMemStreamStore.of(tables).deleteStreams(t, toDelete);
        return false;
    }
}