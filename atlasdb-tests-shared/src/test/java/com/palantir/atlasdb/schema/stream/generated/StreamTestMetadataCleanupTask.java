package com.palantir.atlasdb.schema.stream.generated;

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

public class StreamTestMetadataCleanupTask implements OnCleanupTask {

    private static final Logger log = LoggerFactory.getLogger(StreamTestMetadataCleanupTask.class);

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
                        executeUnreferencedStreamDiagnostics(indexTable, rows);
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

    private static StreamTestStreamMetadataTable.StreamTestStreamMetadataRow convertFromIndexRow(StreamTestStreamIdxTable.StreamTestStreamIdxRow idxRow) {
        return StreamTestStreamMetadataTable.StreamTestStreamMetadataRow.of(idxRow.getId());
    }
    private static Set<Long> convertToIdsForLogging(Set<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow> iteratorExcess) {
        return iteratorExcess.stream()
                .map(StreamTestStreamMetadataTable.StreamTestStreamMetadataRow::getId)
                .collect(Collectors.toSet());
    }

    private static Set<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow> getUnreferencedStreamsByMultimap(StreamTestStreamIdxTable indexTable, Set<StreamTestStreamIdxTable.StreamTestStreamIdxRow> indexRows) {
        Multimap<StreamTestStreamIdxTable.StreamTestStreamIdxRow, StreamTestStreamIdxTable.StreamTestStreamIdxColumnValue> indexValues = indexTable.getRowsMultimap(indexRows);
        Set<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow> presentMetadataRows
                = indexValues.keySet().stream()
                .map(StreamTestMetadataCleanupTask::convertFromIndexRow)
                .collect(Collectors.toSet());
        Set<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow> queriedMetadataRows
                = indexRows.stream()
                .map(StreamTestMetadataCleanupTask::convertFromIndexRow)
                .collect(Collectors.toSet());
        return Sets.difference(queriedMetadataRows, presentMetadataRows);
    }

    private static Set<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow> getUnreferencedStreamsByIterator(StreamTestStreamIdxTable indexTable, Set<StreamTestStreamIdxTable.StreamTestStreamIdxRow> indexRows) {
        Map<StreamTestStreamIdxTable.StreamTestStreamIdxRow, Iterator<StreamTestStreamIdxTable.StreamTestStreamIdxColumnValue>> referenceIteratorByStream
                = indexTable.getRowsColumnRangeIterator(indexRows,
                        BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1));
        Set<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow> unreferencedStreamMetadata
                = KeyedStream.stream(referenceIteratorByStream)
                .filter(valueIterator -> !valueIterator.hasNext())
                .keys() // (authorized)
                .map(StreamTestStreamIdxTable.StreamTestStreamIdxRow::getId)
                .map(StreamTestStreamMetadataTable.StreamTestStreamMetadataRow::of)
                .collect(Collectors.toSet());
        return unreferencedStreamMetadata;
    }

    private static Set<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow> executeUnreferencedStreamDiagnostics(StreamTestStreamIdxTable indexTable, Set<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow> metadataRows) {
        Set<StreamTestStreamIdxTable.StreamTestStreamIdxRow> indexRows = metadataRows.stream()
                .map(StreamTestStreamMetadataTable.StreamTestStreamMetadataRow::getId)
                .map(StreamTestStreamIdxTable.StreamTestStreamIdxRow::of)
                .collect(Collectors.toSet());
        Set<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow> unreferencedStreamsByIterator = getUnreferencedStreamsByIterator(indexTable, indexRows);
        Set<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow> unreferencedStreamsByMultimap = getUnreferencedStreamsByMultimap(indexTable, indexRows);
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
