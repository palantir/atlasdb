package com.palantir.example.profile.schema.generated;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.cleaner.api.CleanupFollowerConfig;
import com.palantir.atlasdb.cleaner.api.ImmutableCleanupFollowerConfig;
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

public class UserPhotosMetadataCleanupTask implements OnCleanupTask {

    private static final Logger log = LoggerFactory.getLogger(UserPhotosMetadataCleanupTask.class);

    private final ProfileTableFactory tables;

    public UserPhotosMetadataCleanupTask(Namespace namespace) {
        tables = ProfileTableFactory.of(namespace);
    }

    @Override
    public boolean cellsCleanedUp(Transaction t, Set<Cell> cells) {
        return cellsCleanedUp(t, cells, ImmutableCleanupFollowerConfig.builder().build());
    }
    @Override
    public boolean cellsCleanedUp(Transaction t, Set<Cell> cells, CleanupFollowerConfig cleanupFollowerConfig) {
        UserPhotosStreamMetadataTable metaTable = tables.getUserPhotosStreamMetadataTable(t);
        Set<UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow> rows = Sets.newHashSetWithExpectedSize(cells.size());
        for (Cell cell : cells) {
            rows.add(UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow.BYTES_HYDRATOR.hydrateFromBytes(cell.getRowName()));
        }
        Map<UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow, StreamMetadata> currentMetadata = metaTable.getMetadatas(rows);
        Set<Long> toDelete = Sets.newHashSet();
        for (Map.Entry<UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow, StreamMetadata> e : currentMetadata.entrySet()) {
            if (e.getValue().getStatus() != Status.STORED) {
                toDelete.add(e.getKey().getId());
            }
        }
        UserPhotosStreamIdxTable indexTable = tables.getUserPhotosStreamIdxTable(t);
        Set<Long> unreferencedStreamIds = findUnreferencedStreams(indexTable, rows);
        if (cleanupFollowerConfig.dangerousRiskOfDataCorruptionEnableCleanupOfUnreferencedStreamsInStreamStoreCleanupTasks()) {
            log.info("Deleting streams {}, which are stored, but we believe to be unreferenced.",
                    SafeArg.of("additionalStreamIds", Sets.difference(unreferencedStreamIds, toDelete)));
            toDelete.addAll(unreferencedStreamIds);
        }
        UserPhotosStreamStore.of(tables).deleteStreams(t, toDelete);
        return false;
    }

    private static Set<Long> findUnreferencedStreams(UserPhotosStreamIdxTable indexTable, Set<UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow> metadataRows) {
        Set<UserPhotosStreamIdxTable.UserPhotosStreamIdxRow> indexRows = metadataRows.stream()
                .map(UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow::getId)
                .map(UserPhotosStreamIdxTable.UserPhotosStreamIdxRow::of)
                .collect(Collectors.toSet());
        Set<Long> unreferencedStreamsByIterator = convertToIds(getUnreferencedStreamsByIterator(indexTable, indexRows));
        Set<Long> unreferencedStreamsByMultimap = convertToIds(getUnreferencedStreamsByMultimap(indexTable, indexRows));
        if (!unreferencedStreamsByIterator.equals(unreferencedStreamsByMultimap)) {
            log.info("We searched for unreferenced streams with methodological inconsistency: iterators claimed we could delete {}, but multimaps {}.",
                    SafeArg.of("unreferencedByIterator", unreferencedStreamsByIterator),
                    SafeArg.of("unreferencedByMultimap", unreferencedStreamsByMultimap));
            return Sets.intersection(unreferencedStreamsByIterator, unreferencedStreamsByMultimap);
        } else {
            log.info("We searched for unreferenced streams and consistently found {}.",
                    SafeArg.of("unreferencedStreamIds", unreferencedStreamsByIterator));
            return unreferencedStreamsByIterator;
        }
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

    private static UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow convertFromIndexRow(UserPhotosStreamIdxTable.UserPhotosStreamIdxRow idxRow) {
        return UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow.of(idxRow.getId());
    }
    private static Set<Long> convertToIds(Set<UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow> iteratorExcess) {
        return iteratorExcess.stream()
                .map(UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow::getId)
                .collect(Collectors.toSet());
    }
}