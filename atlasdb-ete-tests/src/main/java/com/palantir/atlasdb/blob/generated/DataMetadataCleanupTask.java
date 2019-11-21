package com.palantir.atlasdb.blob.generated;

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

public class DataMetadataCleanupTask implements OnCleanupTask {

    private static final Logger log = LoggerFactory.getLogger(DataMetadataCleanupTask.class);

    private final BlobSchemaTableFactory tables;

    public DataMetadataCleanupTask(Namespace namespace) {
        tables = BlobSchemaTableFactory.of(namespace);
    }

    @Override
    public boolean cellsCleanedUp(Transaction t, Set<Cell> cells) {
        return cellsCleanedUp(t, cells, ImmutableCleanupFollowerConfig.builder().build());
    }
    @Override
    public boolean cellsCleanedUp(Transaction t, Set<Cell> cells, CleanupFollowerConfig cleanupFollowerConfig) {
        DataStreamMetadataTable metaTable = tables.getDataStreamMetadataTable(t);
        Set<DataStreamMetadataTable.DataStreamMetadataRow> rows = Sets.newHashSetWithExpectedSize(cells.size());
        for (Cell cell : cells) {
            rows.add(DataStreamMetadataTable.DataStreamMetadataRow.BYTES_HYDRATOR.hydrateFromBytes(cell.getRowName()));
        }
        Map<DataStreamMetadataTable.DataStreamMetadataRow, StreamMetadata> currentMetadata = metaTable.getMetadatas(rows);
        Set<Long> toDelete = Sets.newHashSet();
        for (Map.Entry<DataStreamMetadataTable.DataStreamMetadataRow, StreamMetadata> e : currentMetadata.entrySet()) {
            if (e.getValue().getStatus() != Status.STORED) {
                toDelete.add(e.getKey().getId());
            }
        }
        DataStreamIdxTable indexTable = tables.getDataStreamIdxTable(t);
        Set<Long> unreferencedStreamIds = findUnreferencedStreams(indexTable, rows);
        if (cleanupFollowerConfig.dangerousRiskOfDataCorruptionEnableCleanupOfUnreferencedStreamsInStreamStoreCleanupTasks()) {
            toDelete.addAll(unreferencedStreamIds);
        }
        DataStreamStore.of(tables).deleteStreams(t, toDelete);
        return false;
    }

    private static Set<Long> findUnreferencedStreams(DataStreamIdxTable indexTable, Set<DataStreamMetadataTable.DataStreamMetadataRow> metadataRows) {
        Set<DataStreamIdxTable.DataStreamIdxRow> indexRows = metadataRows.stream()
                .map(DataStreamMetadataTable.DataStreamMetadataRow::getId)
                .map(DataStreamIdxTable.DataStreamIdxRow::of)
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

    private static DataStreamMetadataTable.DataStreamMetadataRow convertFromIndexRow(DataStreamIdxTable.DataStreamIdxRow idxRow) {
        return DataStreamMetadataTable.DataStreamMetadataRow.of(idxRow.getId());
    }
    private static Set<Long> convertToIds(Set<DataStreamMetadataTable.DataStreamMetadataRow> iteratorExcess) {
        return iteratorExcess.stream()
                .map(DataStreamMetadataTable.DataStreamMetadataRow::getId)
                .collect(Collectors.toSet());
    }
}