package com.palantir.atlasdb.blob.generated;

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

public class HotspottyDataMetadataCleanupTask implements OnCleanupTask {

    private static final SafeLogger log = SafeLoggerFactory.get(HotspottyDataMetadataCleanupTask.class);

    private final BlobSchemaTableFactory tables;

    public HotspottyDataMetadataCleanupTask(Namespace namespace) {
        tables = BlobSchemaTableFactory.of(namespace);
    }

    @Override
    public boolean cellsCleanedUp(Transaction t, Set<Cell> cells) {
        HotspottyDataStreamMetadataTable metaTable = tables.getHotspottyDataStreamMetadataTable(t);
        Set<HotspottyDataStreamMetadataTable.HotspottyDataStreamMetadataRow> rows = Sets.newHashSetWithExpectedSize(cells.size());
        for (Cell cell : cells) {
            rows.add(HotspottyDataStreamMetadataTable.HotspottyDataStreamMetadataRow.BYTES_HYDRATOR.hydrateFromBytes(cell.getRowName()));
        }
        HotspottyDataStreamIdxTable indexTable = tables.getHotspottyDataStreamIdxTable(t);
        Set<HotspottyDataStreamMetadataTable.HotspottyDataStreamMetadataRow> rowsWithNoIndexEntries =
                        getUnreferencedStreamsByIterator(indexTable, rows);
        Set<Long> toDelete = new HashSet<>();
        Map<HotspottyDataStreamMetadataTable.HotspottyDataStreamMetadataRow, StreamMetadata> currentMetadata =
                metaTable.getMetadatas(rows);
        for (Map.Entry<HotspottyDataStreamMetadataTable.HotspottyDataStreamMetadataRow, StreamMetadata> e : currentMetadata.entrySet()) {
            if (e.getValue().getStatus() != Status.STORED || rowsWithNoIndexEntries.contains(e.getKey())) {
                toDelete.add(e.getKey().getId());
            }
        }
        HotspottyDataStreamStore.of(tables).deleteStreams(t, toDelete);
        return false;
    }

    private static Set<HotspottyDataStreamMetadataTable.HotspottyDataStreamMetadataRow> getUnreferencedStreamsByIterator(HotspottyDataStreamIdxTable indexTable, Set<HotspottyDataStreamMetadataTable.HotspottyDataStreamMetadataRow> metadataRows) {
        Set<HotspottyDataStreamIdxTable.HotspottyDataStreamIdxRow> indexRows = metadataRows.stream()
                .map(HotspottyDataStreamMetadataTable.HotspottyDataStreamMetadataRow::getId)
                .map(HotspottyDataStreamIdxTable.HotspottyDataStreamIdxRow::of)
                .collect(Collectors.toSet());
        Map<HotspottyDataStreamIdxTable.HotspottyDataStreamIdxRow, Iterator<HotspottyDataStreamIdxTable.HotspottyDataStreamIdxColumnValue>> referenceIteratorByStream
                = indexTable.getRowsColumnRangeIterator(indexRows,
                        BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1));
        return KeyedStream.stream(referenceIteratorByStream)
                .filter(valueIterator -> !valueIterator.hasNext())
                .keys() // (authorized)
                .map(HotspottyDataStreamIdxTable.HotspottyDataStreamIdxRow::getId)
                .map(HotspottyDataStreamMetadataTable.HotspottyDataStreamMetadataRow::of)
                .collect(Collectors.toSet());
    }
}