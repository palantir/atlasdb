package com.palantir.atlasdb.todo.generated;

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

public class SnapshotsMetadataCleanupTask implements OnCleanupTask {

    private static final Logger log = LoggerFactory.getLogger(SnapshotsMetadataCleanupTask.class);

    private final TodoSchemaTableFactory tables;

    public SnapshotsMetadataCleanupTask(Namespace namespace) {
        tables = TodoSchemaTableFactory.of(namespace);
    }

    @Override
    public boolean cellsCleanedUp(Transaction t, Set<Cell> cells) {
        return cellsCleanedUp(t, cells, ImmutableCleanupFollowerConfig.builder().build());
    }
    @Override
    public boolean cellsCleanedUp(Transaction t, Set<Cell> cells, CleanupFollowerConfig cleanupFollowerConfig) {
        SnapshotsStreamMetadataTable metaTable = tables.getSnapshotsStreamMetadataTable(t);
        Set<SnapshotsStreamMetadataTable.SnapshotsStreamMetadataRow> rows = Sets.newHashSetWithExpectedSize(cells.size());
        for (Cell cell : cells) {
            rows.add(SnapshotsStreamMetadataTable.SnapshotsStreamMetadataRow.BYTES_HYDRATOR.hydrateFromBytes(cell.getRowName()));
        }
        Map<SnapshotsStreamMetadataTable.SnapshotsStreamMetadataRow, StreamMetadata> currentMetadata = metaTable.getMetadatas(rows);
        Set<Long> toDelete = Sets.newHashSet();
        for (Map.Entry<SnapshotsStreamMetadataTable.SnapshotsStreamMetadataRow, StreamMetadata> e : currentMetadata.entrySet()) {
            if (e.getValue().getStatus() != Status.STORED) {
                toDelete.add(e.getKey().getId());
            }
        }
        SnapshotsStreamIdxTable indexTable = tables.getSnapshotsStreamIdxTable(t);
        Set<Long> unreferencedStreamIds = findUnreferencedStreams(indexTable, rows);
        if (cleanupFollowerConfig.dangerousRiskOfDataCorruptionEnableCleanupOfUnreferencedStreamsInStreamStoreCleanupTasks()) {
            log.info("Deleting streams {}, which are stored, but we believe to be unreferenced.",
                    SafeArg.of("additionalStreamIds", Sets.difference(unreferencedStreamIds, toDelete)));
            toDelete.addAll(unreferencedStreamIds);
        }
        SnapshotsStreamStore.of(tables).deleteStreams(t, toDelete);
        return false;
    }

    private static Set<Long> findUnreferencedStreams(SnapshotsStreamIdxTable indexTable, Set<SnapshotsStreamMetadataTable.SnapshotsStreamMetadataRow> metadataRows) {
        Set<SnapshotsStreamIdxTable.SnapshotsStreamIdxRow> indexRows = metadataRows.stream()
                .map(SnapshotsStreamMetadataTable.SnapshotsStreamMetadataRow::getId)
                .map(SnapshotsStreamIdxTable.SnapshotsStreamIdxRow::of)
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

    private static Set<SnapshotsStreamMetadataTable.SnapshotsStreamMetadataRow> getUnreferencedStreamsByMultimap(SnapshotsStreamIdxTable indexTable, Set<SnapshotsStreamIdxTable.SnapshotsStreamIdxRow> indexRows) {
        Multimap<SnapshotsStreamIdxTable.SnapshotsStreamIdxRow, SnapshotsStreamIdxTable.SnapshotsStreamIdxColumnValue> indexValues = indexTable.getRowsMultimap(indexRows);
        Set<SnapshotsStreamMetadataTable.SnapshotsStreamMetadataRow> presentMetadataRows
                = indexValues.keySet().stream()
                .map(SnapshotsMetadataCleanupTask::convertFromIndexRow)
                .collect(Collectors.toSet());
        Set<SnapshotsStreamMetadataTable.SnapshotsStreamMetadataRow> queriedMetadataRows
                = indexRows.stream()
                .map(SnapshotsMetadataCleanupTask::convertFromIndexRow)
                .collect(Collectors.toSet());
        return Sets.difference(queriedMetadataRows, presentMetadataRows);
    }

    private static Set<SnapshotsStreamMetadataTable.SnapshotsStreamMetadataRow> getUnreferencedStreamsByIterator(SnapshotsStreamIdxTable indexTable, Set<SnapshotsStreamIdxTable.SnapshotsStreamIdxRow> indexRows) {
        Map<SnapshotsStreamIdxTable.SnapshotsStreamIdxRow, Iterator<SnapshotsStreamIdxTable.SnapshotsStreamIdxColumnValue>> referenceIteratorByStream
                = indexTable.getRowsColumnRangeIterator(indexRows,
                        BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1));
        Set<SnapshotsStreamMetadataTable.SnapshotsStreamMetadataRow> unreferencedStreamMetadata
                = KeyedStream.stream(referenceIteratorByStream)
                .filter(valueIterator -> !valueIterator.hasNext())
                .keys() // (authorized)
                .map(SnapshotsStreamIdxTable.SnapshotsStreamIdxRow::getId)
                .map(SnapshotsStreamMetadataTable.SnapshotsStreamMetadataRow::of)
                .collect(Collectors.toSet());
        return unreferencedStreamMetadata;
    }

    private static SnapshotsStreamMetadataTable.SnapshotsStreamMetadataRow convertFromIndexRow(SnapshotsStreamIdxTable.SnapshotsStreamIdxRow idxRow) {
        return SnapshotsStreamMetadataTable.SnapshotsStreamMetadataRow.of(idxRow.getId());
    }
    private static Set<Long> convertToIds(Set<SnapshotsStreamMetadataTable.SnapshotsStreamMetadataRow> iteratorExcess) {
        return iteratorExcess.stream()
                .map(SnapshotsStreamMetadataTable.SnapshotsStreamMetadataRow::getId)
                .collect(Collectors.toSet());
    }
}