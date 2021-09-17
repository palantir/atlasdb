package com.palantir.atlasdb.todo.generated;

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

public class SnapshotsMetadataCleanupTask implements OnCleanupTask {

    private static final SafeLogger log = SafeLoggerFactory.get(SnapshotsMetadataCleanupTask.class);

    private final TodoSchemaTableFactory tables;

    public SnapshotsMetadataCleanupTask(Namespace namespace) {
        tables = TodoSchemaTableFactory.of(namespace);
    }

    @Override
    public boolean cellsCleanedUp(Transaction t, Set<Cell> cells) {
        SnapshotsStreamMetadataTable metaTable = tables.getSnapshotsStreamMetadataTable(t);
        Set<SnapshotsStreamMetadataTable.SnapshotsStreamMetadataRow> rows = Sets.newHashSetWithExpectedSize(cells.size());
        for (Cell cell : cells) {
            rows.add(SnapshotsStreamMetadataTable.SnapshotsStreamMetadataRow.BYTES_HYDRATOR.hydrateFromBytes(cell.getRowName()));
        }
        SnapshotsStreamIdxTable indexTable = tables.getSnapshotsStreamIdxTable(t);
        Set<SnapshotsStreamMetadataTable.SnapshotsStreamMetadataRow> rowsWithNoIndexEntries =
                        getUnreferencedStreamsByIterator(indexTable, rows);
        Set<Long> toDelete = new HashSet<>();
        Map<SnapshotsStreamMetadataTable.SnapshotsStreamMetadataRow, StreamMetadata> currentMetadata =
                metaTable.getMetadatas(rows);
        for (Map.Entry<SnapshotsStreamMetadataTable.SnapshotsStreamMetadataRow, StreamMetadata> e : currentMetadata.entrySet()) {
            if (e.getValue().getStatus() != Status.STORED || rowsWithNoIndexEntries.contains(e.getKey())) {
                toDelete.add(e.getKey().getId());
            }
        }
        SnapshotsStreamStore.of(tables).deleteStreams(t, toDelete);
        return false;
    }

    private static Set<SnapshotsStreamMetadataTable.SnapshotsStreamMetadataRow> getUnreferencedStreamsByIterator(SnapshotsStreamIdxTable indexTable, Set<SnapshotsStreamMetadataTable.SnapshotsStreamMetadataRow> metadataRows) {
        Set<SnapshotsStreamIdxTable.SnapshotsStreamIdxRow> indexRows = metadataRows.stream()
                .map(SnapshotsStreamMetadataTable.SnapshotsStreamMetadataRow::getId)
                .map(SnapshotsStreamIdxTable.SnapshotsStreamIdxRow::of)
                .collect(Collectors.toSet());
        Map<SnapshotsStreamIdxTable.SnapshotsStreamIdxRow, Iterator<SnapshotsStreamIdxTable.SnapshotsStreamIdxColumnValue>> referenceIteratorByStream
                = indexTable.getRowsColumnRangeIterator(indexRows,
                        BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1));
        return KeyedStream.stream(referenceIteratorByStream)
                .filter(valueIterator -> !valueIterator.hasNext())
                .keys() // (authorized)
                .map(SnapshotsStreamIdxTable.SnapshotsStreamIdxRow::getId)
                .map(SnapshotsStreamMetadataTable.SnapshotsStreamMetadataRow::of)
                .collect(Collectors.toSet());
    }
}