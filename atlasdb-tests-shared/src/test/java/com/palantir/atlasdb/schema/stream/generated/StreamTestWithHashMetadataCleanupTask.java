package com.palantir.atlasdb.schema.stream.generated;

import java.util.Iterator;
import java.util.Map;
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

public class StreamTestWithHashMetadataCleanupTask implements OnCleanupTask {

    private static final Logger log = LoggerFactory.getLogger(StreamTestWithHashMetadataCleanupTask.class);

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
        Set<StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow> rowsWithNoIndexEntries =
                        executeUnreferencedStreamDiagnostics(indexTable, rows);
        Set<Long> toDelete = Sets.newHashSet();
        Map<StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow, StreamMetadata> currentMetadata =
                metaTable.getMetadatas(rows);
        for (Map.Entry<StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow, StreamMetadata> e : currentMetadata.entrySet()) {
            if (e.getValue().getStatus() != Status.STORED || rowsWithNoIndexEntries.contains(e.getKey())) {
                toDelete.add(e.getKey().getId());
            }
        }
        StreamTestWithHashStreamStore.of(tables).deleteStreams(t, toDelete);
        return false;
    }

    private static StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow convertFromIndexRow(StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxRow idxRow) {
        return StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow.of(idxRow.getId());
    }
    private static Set<Long> convertToIdsForLogging(Set<StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow> iteratorExcess) {
        return iteratorExcess.stream()
                .map(StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow::getId)
                .collect(Collectors.toSet());
    }

    private static Set<StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow> getUnreferencedStreamsByMultimap(StreamTestWithHashStreamIdxTable indexTable, Set<StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxRow> indexRows) {
        Multimap<StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxRow, StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxColumnValue> indexValues = indexTable.getRowsMultimap(indexRows);
        Set<StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow> presentMetadataRows
                = indexValues.keySet().stream()
                .map(StreamTestWithHashMetadataCleanupTask::convertFromIndexRow)
                .collect(Collectors.toSet());
        Set<StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow> queriedMetadataRows
                = indexRows.stream()
                .map(StreamTestWithHashMetadataCleanupTask::convertFromIndexRow)
                .collect(Collectors.toSet());
        return Sets.difference(queriedMetadataRows, presentMetadataRows);
    }

    private static Set<StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow> getUnreferencedStreamsByIterator(StreamTestWithHashStreamIdxTable indexTable, Set<StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxRow> indexRows) {
        Map<StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxRow, Iterator<StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxColumnValue>> referenceIteratorByStream
                = indexTable.getRowsColumnRangeIterator(indexRows,
                        BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1));
        Set<StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow> unreferencedStreamMetadata
                = KeyedStream.stream(referenceIteratorByStream)
                .filter(valueIterator -> !valueIterator.hasNext())
                .keys() // (authorized)
                .map(StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxRow::getId)
                .map(StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow::of)
                .collect(Collectors.toSet());
        return unreferencedStreamMetadata;
    }

    private static Set<StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow> executeUnreferencedStreamDiagnostics(StreamTestWithHashStreamIdxTable indexTable, Set<StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow> metadataRows) {
        Set<StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxRow> indexRows = metadataRows.stream()
                .map(StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow::getId)
                .map(StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxRow::of)
                .collect(Collectors.toSet());
        Set<StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow> unreferencedStreamsByIterator = getUnreferencedStreamsByIterator(indexTable, indexRows);
        Set<StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow> unreferencedStreamsByMultimap = getUnreferencedStreamsByMultimap(indexTable, indexRows);
        if (!unreferencedStreamsByIterator.equals(unreferencedStreamsByMultimap)) {
            log.info("We searched for unreferenced streams with methodological inconsistency: iterators claimed we could delete {}, but multimaps {}.",
                    SafeArg.of("unreferencedByIterator", convertToIdsForLogging(unreferencedStreamsByIterator)),
                    SafeArg.of("unreferencedByMultimap", convertToIdsForLogging(unreferencedStreamsByMultimap)));
            return Sets.newHashSet();
        } else {
            log.info("We searched for unreferenced streams and consistently found {}.",
                    SafeArg.of("unreferencedStreamIds", convertToIdsForLogging(unreferencedStreamsByIterator)));
            return unreferencedStreamsByIterator;
        }
    }
}