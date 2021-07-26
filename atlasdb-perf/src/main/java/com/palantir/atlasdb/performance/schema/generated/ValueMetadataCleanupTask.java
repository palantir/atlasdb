package com.palantir.atlasdb.performance.schema.generated;

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

public class ValueMetadataCleanupTask implements OnCleanupTask {

    private static final SafeLogger log = SafeLoggerFactory.get(ValueMetadataCleanupTask.class);

    private final StreamTestTableFactory tables;

    public ValueMetadataCleanupTask(Namespace namespace) {
        tables = StreamTestTableFactory.of(namespace);
    }

    @Override
    public boolean cellsCleanedUp(Transaction t, Set<Cell> cells) {
        ValueStreamMetadataTable metaTable = tables.getValueStreamMetadataTable(t);
        Set<ValueStreamMetadataTable.ValueStreamMetadataRow> rows = Sets.newHashSetWithExpectedSize(cells.size());
        for (Cell cell : cells) {
            rows.add(ValueStreamMetadataTable.ValueStreamMetadataRow.BYTES_HYDRATOR.hydrateFromBytes(cell.getRowName()));
        }
        ValueStreamIdxTable indexTable = tables.getValueStreamIdxTable(t);
        Set<ValueStreamMetadataTable.ValueStreamMetadataRow> rowsWithNoIndexEntries =
                        executeUnreferencedStreamDiagnostics(indexTable, rows);
        Set<Long> toDelete = new HashSet<>();
        Map<ValueStreamMetadataTable.ValueStreamMetadataRow, StreamMetadata> currentMetadata =
                metaTable.getMetadatas(rows);
        for (Map.Entry<ValueStreamMetadataTable.ValueStreamMetadataRow, StreamMetadata> e : currentMetadata.entrySet()) {
            if (e.getValue().getStatus() != Status.STORED || rowsWithNoIndexEntries.contains(e.getKey())) {
                toDelete.add(e.getKey().getId());
            }
        }
        ValueStreamStore.of(tables).deleteStreams(t, toDelete);
        return false;
    }

    private static ValueStreamMetadataTable.ValueStreamMetadataRow convertFromIndexRow(ValueStreamIdxTable.ValueStreamIdxRow idxRow) {
        return ValueStreamMetadataTable.ValueStreamMetadataRow.of(idxRow.getId());
    }
    private static Set<Long> convertToIdsForLogging(Set<ValueStreamMetadataTable.ValueStreamMetadataRow> iteratorExcess) {
        return iteratorExcess.stream()
                .map(ValueStreamMetadataTable.ValueStreamMetadataRow::getId)
                .collect(Collectors.toSet());
    }

    private static Set<ValueStreamMetadataTable.ValueStreamMetadataRow> getUnreferencedStreamsByMultimap(ValueStreamIdxTable indexTable, Set<ValueStreamIdxTable.ValueStreamIdxRow> indexRows) {
        Multimap<ValueStreamIdxTable.ValueStreamIdxRow, ValueStreamIdxTable.ValueStreamIdxColumnValue> indexValues = indexTable.getRowsMultimap(indexRows);
        Set<ValueStreamMetadataTable.ValueStreamMetadataRow> presentMetadataRows
                = indexValues.keySet().stream()
                .map(ValueMetadataCleanupTask::convertFromIndexRow)
                .collect(Collectors.toSet());
        Set<ValueStreamMetadataTable.ValueStreamMetadataRow> queriedMetadataRows
                = indexRows.stream()
                .map(ValueMetadataCleanupTask::convertFromIndexRow)
                .collect(Collectors.toSet());
        return Sets.difference(queriedMetadataRows, presentMetadataRows);
    }

    private static Set<ValueStreamMetadataTable.ValueStreamMetadataRow> getUnreferencedStreamsByIterator(ValueStreamIdxTable indexTable, Set<ValueStreamIdxTable.ValueStreamIdxRow> indexRows) {
        Map<ValueStreamIdxTable.ValueStreamIdxRow, Iterator<ValueStreamIdxTable.ValueStreamIdxColumnValue>> referenceIteratorByStream
                = indexTable.getRowsColumnRangeIterator(indexRows,
                        BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1));
        Set<ValueStreamMetadataTable.ValueStreamMetadataRow> unreferencedStreamMetadata
                = KeyedStream.stream(referenceIteratorByStream)
                .filter(valueIterator -> !valueIterator.hasNext())
                .keys() // (authorized)
                .map(ValueStreamIdxTable.ValueStreamIdxRow::getId)
                .map(ValueStreamMetadataTable.ValueStreamMetadataRow::of)
                .collect(Collectors.toSet());
        return unreferencedStreamMetadata;
    }

    private static Set<ValueStreamMetadataTable.ValueStreamMetadataRow> executeUnreferencedStreamDiagnostics(ValueStreamIdxTable indexTable, Set<ValueStreamMetadataTable.ValueStreamMetadataRow> metadataRows) {
        Set<ValueStreamIdxTable.ValueStreamIdxRow> indexRows = metadataRows.stream()
                .map(ValueStreamMetadataTable.ValueStreamMetadataRow::getId)
                .map(ValueStreamIdxTable.ValueStreamIdxRow::of)
                .collect(Collectors.toSet());
        Set<ValueStreamMetadataTable.ValueStreamMetadataRow> unreferencedStreamsByIterator = getUnreferencedStreamsByIterator(indexTable, indexRows);
        Set<ValueStreamMetadataTable.ValueStreamMetadataRow> unreferencedStreamsByMultimap = getUnreferencedStreamsByMultimap(indexTable, indexRows);
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