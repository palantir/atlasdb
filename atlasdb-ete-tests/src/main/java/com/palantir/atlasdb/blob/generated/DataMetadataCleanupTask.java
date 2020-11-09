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
                        executeUnreferencedStreamDiagnostics(indexTable, rows);
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

    private static DataStreamMetadataTable.DataStreamMetadataRow convertFromIndexRow(DataStreamIdxTable.DataStreamIdxRow idxRow) {
        return DataStreamMetadataTable.DataStreamMetadataRow.of(idxRow.getId());
    }
    private static Set<Long> convertToIdsForLogging(Set<DataStreamMetadataTable.DataStreamMetadataRow> iteratorExcess) {
        return iteratorExcess.stream()
                .map(DataStreamMetadataTable.DataStreamMetadataRow::getId)
                .collect(Collectors.toSet());
    }

    private static Set<DataStreamMetadataTable.DataStreamMetadataRow> getUnreferencedStreamsByMultimap(DataStreamIdxTable indexTable, Set<DataStreamIdxTable.DataStreamIdxRow> indexRows) {
        Multimap<DataStreamIdxTable.DataStreamIdxRow, DataStreamIdxTable.DataStreamIdxColumnValue> indexValues = indexTable.getRowsMultimap(indexRows);
        Set<DataStreamMetadataTable.DataStreamMetadataRow> presentMetadataRows
                = indexValues.keySet().stream()
                .map(DataMetadataCleanupTask::convertFromIndexRow)
                .collect(Collectors.toSet());
        Set<DataStreamMetadataTable.DataStreamMetadataRow> queriedMetadataRows
                = indexRows.stream()
                .map(DataMetadataCleanupTask::convertFromIndexRow)
                .collect(Collectors.toSet());
        return Sets.difference(queriedMetadataRows, presentMetadataRows);
    }

    private static Set<DataStreamMetadataTable.DataStreamMetadataRow> getUnreferencedStreamsByIterator(DataStreamIdxTable indexTable, Set<DataStreamIdxTable.DataStreamIdxRow> indexRows) {
        Map<DataStreamIdxTable.DataStreamIdxRow, Iterator<DataStreamIdxTable.DataStreamIdxColumnValue>> referenceIteratorByStream
                = indexTable.getRowsColumnRangeIterator(indexRows,
                        BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1));
        Set<DataStreamMetadataTable.DataStreamMetadataRow> unreferencedStreamMetadata
                = KeyedStream.stream(referenceIteratorByStream)
                .filter(valueIterator -> !valueIterator.hasNext())
                .keys() // (authorized)
                .map(DataStreamIdxTable.DataStreamIdxRow::getId)
                .map(DataStreamMetadataTable.DataStreamMetadataRow::of)
                .collect(Collectors.toSet());
        return unreferencedStreamMetadata;
    }

    private static Set<DataStreamMetadataTable.DataStreamMetadataRow> executeUnreferencedStreamDiagnostics(DataStreamIdxTable indexTable, Set<DataStreamMetadataTable.DataStreamMetadataRow> metadataRows) {
        Set<DataStreamIdxTable.DataStreamIdxRow> indexRows = metadataRows.stream()
                .map(DataStreamMetadataTable.DataStreamMetadataRow::getId)
                .map(DataStreamIdxTable.DataStreamIdxRow::of)
                .collect(Collectors.toSet());
        Set<DataStreamMetadataTable.DataStreamMetadataRow> unreferencedStreamsByIterator = getUnreferencedStreamsByIterator(indexTable, indexRows);
        Set<DataStreamMetadataTable.DataStreamMetadataRow> unreferencedStreamsByMultimap = getUnreferencedStreamsByMultimap(indexTable, indexRows);
        if (!unreferencedStreamsByIterator.equals(unreferencedStreamsByMultimap)) {
            log.info("We searched for unreferenced streams with methodological inconsistency: iterators claimed we could delete {}, but multimaps {}.",
                    SafeArg.of("unreferencedByIterator", convertToIdsForLogging(unreferencedStreamsByIterator)),
                    SafeArg.of("unreferencedByMultimap", convertToIdsForLogging(unreferencedStreamsByMultimap)));
            return new HashSet<>();
        } else {
            log.info("We searched for unreferenced streams and consistently found {}.",
                    SafeArg.of("unreferencedStreamIds", convertToIdsForLogging(unreferencedStreamsByIterator)));
            return unreferencedStreamsByIterator;
        }
    }
}