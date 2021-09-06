package com.palantir.atlasdb.blob.generated;

import java.util.Iterator;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Multimap;
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
import com.palantir.logsafe.SafeArg;

public class DataMetadataCleanupTask implements OnCleanupTask {

    private static final Logger log = LoggerFactory.getLogger(DataMetadataCleanupTask.class);

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
        Set<DataStreamMetadataTable.DataStreamMetadataRow> rowsWithNoIndexEntries =
                        getUnreferencedStreamsByIterator(indexTable, rows);
        Set<Long> toDelete = new HashSet<>();
        Map<DataStreamMetadataTable.DataStreamMetadataRow, StreamMetadata> currentMetadata =
                metaTable.getMetadatas(rows);
        for (Map.Entry<DataStreamMetadataTable.DataStreamMetadataRow, StreamMetadata> e : currentMetadata.entrySet()) {
            if (e.getValue().getStatus() != Status.STORED || rowsWithNoIndexEntries.contains(e.getKey())) {
                toDelete.add(e.getKey().getId());
            }
        }
        DataStreamStore.of(tables).deleteStreams(t, toDelete);
        return false;
    }

    private static Set<DataStreamMetadataTable.DataStreamMetadataRow> getUnreferencedStreamsByIterator(DataStreamIdxTable indexTable, Set<DataStreamMetadataTable.DataStreamMetadataRow> metadataRows) {
        Set<DataStreamIdxTable.DataStreamIdxRow> indexRows = metadataRows.stream()
                .map(DataStreamMetadataTable.DataStreamMetadataRow::getId)
                .map(DataStreamIdxTable.DataStreamIdxRow::of)
                .collect(Collectors.toSet());
        Map<DataStreamIdxTable.DataStreamIdxRow, Iterator<DataStreamIdxTable.DataStreamIdxColumnValue>> referenceIteratorByStream
                = indexTable.getRowsColumnRangeIterator(indexRows,
                        BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1));
        return KeyedStream.stream(referenceIteratorByStream)
                .filter(valueIterator -> !valueIterator.hasNext())
                .keys() // (authorized)
                .map(DataStreamIdxTable.DataStreamIdxRow::getId)
                .map(DataStreamMetadataTable.DataStreamMetadataRow::of)
                .collect(Collectors.toSet());
    }
}