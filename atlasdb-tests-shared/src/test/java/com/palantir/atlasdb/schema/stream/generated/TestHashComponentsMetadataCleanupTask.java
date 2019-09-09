package com.palantir.atlasdb.schema.stream.generated;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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

public class TestHashComponentsMetadataCleanupTask implements OnCleanupTask {

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
        Set<TestHashComponentsStreamIdxTable.TestHashComponentsStreamIdxRow> indexRows = rows.stream()
                .map(TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow::getId)
                .map(TestHashComponentsStreamIdxTable.TestHashComponentsStreamIdxRow::of)
                .collect(Collectors.toSet());
        Map<TestHashComponentsStreamIdxTable.TestHashComponentsStreamIdxRow, Iterator<TestHashComponentsStreamIdxTable.TestHashComponentsStreamIdxColumnValue>> referenceIteratorByStream
                = indexTable.getRowsColumnRangeIterator(indexRows,
                        BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1));
        Set<TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow> streamsWithNoReferences
                = KeyedStream.stream(referenceIteratorByStream)
                .filter(valueIterator -> !valueIterator.hasNext())
                .keys() // (authorized)
                .map(TestHashComponentsStreamIdxTable.TestHashComponentsStreamIdxRow::getId)
                .map(TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow::of)
                .collect(Collectors.toSet());
        Map<TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow, StreamMetadata> currentMetadata = metaTable.getMetadatas(rows);
        Set<Long> toDelete = Sets.newHashSet();
        for (Map.Entry<TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow, StreamMetadata> e : currentMetadata.entrySet()) {
            if (e.getValue().getStatus() != Status.STORED || streamsWithNoReferences.contains(e.getKey())) {
                toDelete.add(e.getKey().getId());
            }
        }
        TestHashComponentsStreamStore.of(tables).deleteStreams(t, toDelete);
        return false;
    }
}