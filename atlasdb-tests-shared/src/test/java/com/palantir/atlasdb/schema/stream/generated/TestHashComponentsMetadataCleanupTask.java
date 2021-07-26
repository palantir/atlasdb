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

public class TestHashComponentsMetadataCleanupTask implements OnCleanupTask {

    private static final SafeLogger log = SafeLoggerFactory.get(TestHashComponentsMetadataCleanupTask.class);

    private final StreamTestTableFactory tables;

    public TestHashComponentsMetadataCleanupTask(Namespace namespace) {
        tables = StreamTestTableFactory.of(namespace);
    }

    @Override
    public boolean cellsCleanedUp(Transaction t, Set<Cell> cells) {
        TestHashComponentsStreamMetadataTable metaTable = tables.getTestHashComponentsStreamMetadataTable(t);
        Set<TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow> rows = Sets.newHashSetWithExpectedSize(cells.size());
        for (Cell cell : cells) {
            rows.add(TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow.BYTES_HYDRATOR.hydrateFromBytes(cell.getRowName()));
        }
        TestHashComponentsStreamIdxTable indexTable = tables.getTestHashComponentsStreamIdxTable(t);
        Set<TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow> rowsWithNoIndexEntries =
                        executeUnreferencedStreamDiagnostics(indexTable, rows);
        Set<Long> toDelete = new HashSet<>();
        Map<TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow, StreamMetadata> currentMetadata =
                metaTable.getMetadatas(rows);
        for (Map.Entry<TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow, StreamMetadata> e : currentMetadata.entrySet()) {
            if (e.getValue().getStatus() != Status.STORED || rowsWithNoIndexEntries.contains(e.getKey())) {
                toDelete.add(e.getKey().getId());
            }
        }
        TestHashComponentsStreamStore.of(tables).deleteStreams(t, toDelete);
        return false;
    }

    private static TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow convertFromIndexRow(TestHashComponentsStreamIdxTable.TestHashComponentsStreamIdxRow idxRow) {
        return TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow.of(idxRow.getId());
    }
    private static Set<Long> convertToIdsForLogging(Set<TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow> iteratorExcess) {
        return iteratorExcess.stream()
                .map(TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow::getId)
                .collect(Collectors.toSet());
    }

    private static Set<TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow> getUnreferencedStreamsByMultimap(TestHashComponentsStreamIdxTable indexTable, Set<TestHashComponentsStreamIdxTable.TestHashComponentsStreamIdxRow> indexRows) {
        Multimap<TestHashComponentsStreamIdxTable.TestHashComponentsStreamIdxRow, TestHashComponentsStreamIdxTable.TestHashComponentsStreamIdxColumnValue> indexValues = indexTable.getRowsMultimap(indexRows);
        Set<TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow> presentMetadataRows
                = indexValues.keySet().stream()
                .map(TestHashComponentsMetadataCleanupTask::convertFromIndexRow)
                .collect(Collectors.toSet());
        Set<TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow> queriedMetadataRows
                = indexRows.stream()
                .map(TestHashComponentsMetadataCleanupTask::convertFromIndexRow)
                .collect(Collectors.toSet());
        return Sets.difference(queriedMetadataRows, presentMetadataRows);
    }

    private static Set<TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow> getUnreferencedStreamsByIterator(TestHashComponentsStreamIdxTable indexTable, Set<TestHashComponentsStreamIdxTable.TestHashComponentsStreamIdxRow> indexRows) {
        Map<TestHashComponentsStreamIdxTable.TestHashComponentsStreamIdxRow, Iterator<TestHashComponentsStreamIdxTable.TestHashComponentsStreamIdxColumnValue>> referenceIteratorByStream
                = indexTable.getRowsColumnRangeIterator(indexRows,
                        BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1));
        Set<TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow> unreferencedStreamMetadata
                = KeyedStream.stream(referenceIteratorByStream)
                .filter(valueIterator -> !valueIterator.hasNext())
                .keys() // (authorized)
                .map(TestHashComponentsStreamIdxTable.TestHashComponentsStreamIdxRow::getId)
                .map(TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow::of)
                .collect(Collectors.toSet());
        return unreferencedStreamMetadata;
    }

    private static Set<TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow> executeUnreferencedStreamDiagnostics(TestHashComponentsStreamIdxTable indexTable, Set<TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow> metadataRows) {
        Set<TestHashComponentsStreamIdxTable.TestHashComponentsStreamIdxRow> indexRows = metadataRows.stream()
                .map(TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow::getId)
                .map(TestHashComponentsStreamIdxTable.TestHashComponentsStreamIdxRow::of)
                .collect(Collectors.toSet());
        Set<TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow> unreferencedStreamsByIterator = getUnreferencedStreamsByIterator(indexTable, indexRows);
        Set<TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow> unreferencedStreamsByMultimap = getUnreferencedStreamsByMultimap(indexTable, indexRows);
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
