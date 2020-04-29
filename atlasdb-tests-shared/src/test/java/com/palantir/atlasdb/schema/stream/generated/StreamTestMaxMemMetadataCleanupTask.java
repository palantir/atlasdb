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

public class StreamTestMaxMemMetadataCleanupTask implements OnCleanupTask {

    private static final Logger log = LoggerFactory.getLogger(StreamTestMaxMemMetadataCleanupTask.class);

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
        Set<StreamTestMaxMemStreamMetadataTable.StreamTestMaxMemStreamMetadataRow> rowsWithNoIndexEntries =
                        executeUnreferencedStreamDiagnostics(indexTable, rows);
        Set<Long> toDelete = Sets.newHashSet();
        Map<StreamTestMaxMemStreamMetadataTable.StreamTestMaxMemStreamMetadataRow, StreamMetadata> currentMetadata =
                metaTable.getMetadatas(rows);
        for (Map.Entry<StreamTestMaxMemStreamMetadataTable.StreamTestMaxMemStreamMetadataRow, StreamMetadata> e : currentMetadata.entrySet()) {
            if (e.getValue().getStatus() != Status.STORED || rowsWithNoIndexEntries.contains(e.getKey())) {
                toDelete.add(e.getKey().getId());
            }
        }
        StreamTestMaxMemStreamStore.of(tables).deleteStreams(t, toDelete);
        return false;
    }

    private static StreamTestMaxMemStreamMetadataTable.StreamTestMaxMemStreamMetadataRow convertFromIndexRow(StreamTestMaxMemStreamIdxTable.StreamTestMaxMemStreamIdxRow idxRow) {
        return StreamTestMaxMemStreamMetadataTable.StreamTestMaxMemStreamMetadataRow.of(idxRow.getId());
    }
    private static Set<Long> convertToIdsForLogging(Set<StreamTestMaxMemStreamMetadataTable.StreamTestMaxMemStreamMetadataRow> iteratorExcess) {
        return iteratorExcess.stream()
                .map(StreamTestMaxMemStreamMetadataTable.StreamTestMaxMemStreamMetadataRow::getId)
                .collect(Collectors.toSet());
    }

    private static Set<StreamTestMaxMemStreamMetadataTable.StreamTestMaxMemStreamMetadataRow> getUnreferencedStreamsByMultimap(StreamTestMaxMemStreamIdxTable indexTable, Set<StreamTestMaxMemStreamIdxTable.StreamTestMaxMemStreamIdxRow> indexRows) {
        Multimap<StreamTestMaxMemStreamIdxTable.StreamTestMaxMemStreamIdxRow, StreamTestMaxMemStreamIdxTable.StreamTestMaxMemStreamIdxColumnValue> indexValues = indexTable.getRowsMultimap(indexRows);
        Set<StreamTestMaxMemStreamMetadataTable.StreamTestMaxMemStreamMetadataRow> presentMetadataRows
                = indexValues.keySet().stream()
                .map(StreamTestMaxMemMetadataCleanupTask::convertFromIndexRow)
                .collect(Collectors.toSet());
        Set<StreamTestMaxMemStreamMetadataTable.StreamTestMaxMemStreamMetadataRow> queriedMetadataRows
                = indexRows.stream()
                .map(StreamTestMaxMemMetadataCleanupTask::convertFromIndexRow)
                .collect(Collectors.toSet());
        return Sets.difference(queriedMetadataRows, presentMetadataRows);
    }

    private static Set<StreamTestMaxMemStreamMetadataTable.StreamTestMaxMemStreamMetadataRow> getUnreferencedStreamsByIterator(StreamTestMaxMemStreamIdxTable indexTable, Set<StreamTestMaxMemStreamIdxTable.StreamTestMaxMemStreamIdxRow> indexRows) {
        Map<StreamTestMaxMemStreamIdxTable.StreamTestMaxMemStreamIdxRow, Iterator<StreamTestMaxMemStreamIdxTable.StreamTestMaxMemStreamIdxColumnValue>> referenceIteratorByStream
                = indexTable.getRowsColumnRangeIterator(indexRows,
                        BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1));
        Set<StreamTestMaxMemStreamMetadataTable.StreamTestMaxMemStreamMetadataRow> unreferencedStreamMetadata
                = KeyedStream.stream(referenceIteratorByStream)
                .filter(valueIterator -> !valueIterator.hasNext())
                .keys() // (authorized)
                .map(StreamTestMaxMemStreamIdxTable.StreamTestMaxMemStreamIdxRow::getId)
                .map(StreamTestMaxMemStreamMetadataTable.StreamTestMaxMemStreamMetadataRow::of)
                .collect(Collectors.toSet());
        return unreferencedStreamMetadata;
    }

    private static Set<StreamTestMaxMemStreamMetadataTable.StreamTestMaxMemStreamMetadataRow> executeUnreferencedStreamDiagnostics(StreamTestMaxMemStreamIdxTable indexTable, Set<StreamTestMaxMemStreamMetadataTable.StreamTestMaxMemStreamMetadataRow> metadataRows) {
        Set<StreamTestMaxMemStreamIdxTable.StreamTestMaxMemStreamIdxRow> indexRows = metadataRows.stream()
                .map(StreamTestMaxMemStreamMetadataTable.StreamTestMaxMemStreamMetadataRow::getId)
                .map(StreamTestMaxMemStreamIdxTable.StreamTestMaxMemStreamIdxRow::of)
                .collect(Collectors.toSet());
        Set<StreamTestMaxMemStreamMetadataTable.StreamTestMaxMemStreamMetadataRow> unreferencedStreamsByIterator = getUnreferencedStreamsByIterator(indexTable, indexRows);
        Set<StreamTestMaxMemStreamMetadataTable.StreamTestMaxMemStreamMetadataRow> unreferencedStreamsByMultimap = getUnreferencedStreamsByMultimap(indexTable, indexRows);
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