package com.palantir.example.profile.schema.generated;

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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserPhotosMetadataCleanupTask implements OnCleanupTask {

    private static final Logger log = LoggerFactory.getLogger(UserPhotosMetadataCleanupTask.class);

    private final ProfileTableFactory tables;

    public UserPhotosMetadataCleanupTask(Namespace namespace) {
        tables = ProfileTableFactory.of(namespace);
    }

    @Override
    public boolean cellsCleanedUp(Transaction t, Set<Cell> cells) {
        UserPhotosStreamMetadataTable metaTable = tables.getUserPhotosStreamMetadataTable(t);
        Set<UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow> rows = Sets.newHashSetWithExpectedSize(cells.size());
        for (Cell cell : cells) {
            rows.add(UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow.BYTES_HYDRATOR.hydrateFromBytes(cell.getRowName()));
        }
        UserPhotosStreamIdxTable indexTable = tables.getUserPhotosStreamIdxTable(t);
        Set<UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow> rowsWithNoIndexEntries =
                        executeUnreferencedStreamDiagnostics(indexTable, rows);
        Set<Long> toDelete = new HashSet<>();
        Map<UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow, StreamMetadata> currentMetadata =
                metaTable.getMetadatas(rows);
        for (Map.Entry<UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow, StreamMetadata> e : currentMetadata.entrySet()) {
            if (e.getValue().getStatus() != Status.STORED || rowsWithNoIndexEntries.contains(e.getKey())) {
                toDelete.add(e.getKey().getId());
            }
        }
        UserPhotosStreamStore.of(tables).deleteStreams(t, toDelete);
        return false;
    }

    private static UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow convertFromIndexRow(UserPhotosStreamIdxTable.UserPhotosStreamIdxRow idxRow) {
        return UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow.of(idxRow.getId());
    }
    private static Set<Long> convertToIdsForLogging(Set<UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow> iteratorExcess) {
        return iteratorExcess.stream()
                .map(UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow::getId)
                .collect(Collectors.toSet());
    }

    private static Set<UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow> getUnreferencedStreamsByMultimap(UserPhotosStreamIdxTable indexTable, Set<UserPhotosStreamIdxTable.UserPhotosStreamIdxRow> indexRows) {
        Multimap<UserPhotosStreamIdxTable.UserPhotosStreamIdxRow, UserPhotosStreamIdxTable.UserPhotosStreamIdxColumnValue> indexValues = indexTable.getRowsMultimap(indexRows);
        Set<UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow> presentMetadataRows
                = indexValues.keySet().stream()
                .map(UserPhotosMetadataCleanupTask::convertFromIndexRow)
                .collect(Collectors.toSet());
        Set<UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow> queriedMetadataRows
                = indexRows.stream()
                .map(UserPhotosMetadataCleanupTask::convertFromIndexRow)
                .collect(Collectors.toSet());
        return Sets.difference(queriedMetadataRows, presentMetadataRows);
    }

    private static Set<UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow> getUnreferencedStreamsByIterator(UserPhotosStreamIdxTable indexTable, Set<UserPhotosStreamIdxTable.UserPhotosStreamIdxRow> indexRows) {
        Map<UserPhotosStreamIdxTable.UserPhotosStreamIdxRow, Iterator<UserPhotosStreamIdxTable.UserPhotosStreamIdxColumnValue>> referenceIteratorByStream
                = indexTable.getRowsColumnRangeIterator(indexRows,
                        BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1));
        Set<UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow> unreferencedStreamMetadata
                = KeyedStream.stream(referenceIteratorByStream)
                .filter(valueIterator -> !valueIterator.hasNext())
                .keys() // (authorized)
                .map(UserPhotosStreamIdxTable.UserPhotosStreamIdxRow::getId)
                .map(UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow::of)
                .collect(Collectors.toSet());
        return unreferencedStreamMetadata;
    }

    private static Set<UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow> executeUnreferencedStreamDiagnostics(UserPhotosStreamIdxTable indexTable, Set<UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow> metadataRows) {
        Set<UserPhotosStreamIdxTable.UserPhotosStreamIdxRow> indexRows = metadataRows.stream()
                .map(UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow::getId)
                .map(UserPhotosStreamIdxTable.UserPhotosStreamIdxRow::of)
                .collect(Collectors.toSet());
        Set<UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow> unreferencedStreamsByIterator = getUnreferencedStreamsByIterator(indexTable, indexRows);
        Set<UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow> unreferencedStreamsByMultimap = getUnreferencedStreamsByMultimap(indexTable, indexRows);
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