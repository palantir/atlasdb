package com.palantir.atlasdb.schema.stream.generated;

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
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamTestMetadataCleanupTask implements OnCleanupTask {

    private static final SafeLogger log = SafeLoggerFactory.get(StreamTestMetadataCleanupTask.class);

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
        return KeyedStream.stream(referenceIteratorByStream)
                .filter(valueIterator -> !valueIterator.hasNext())
                .keys() // (authorized)
                .map(StreamTestStreamIdxTable.StreamTestStreamIdxRow::getId)
                .map(StreamTestStreamMetadataTable.StreamTestStreamMetadataRow::of)
                .collect(Collectors.toSet());
    }
}