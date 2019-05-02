package com.palantir.atlasdb.blob.generated;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.cleaner.api.OnCleanupTask;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.protos.generated.StreamPersistence.Status;
import com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.Transaction;

public class DataMetadataCleanupTask implements OnCleanupTask {

    private final BlobSchemaTableFactory tables;

    public DataMetadataCleanupTask(Namespace namespace) {
        tables = BlobSchemaTableFactory.of(namespace);
    }

    @Override
    public boolean cellsCleanedUp(Transaction t, Set<Cell> cells) {
        DataStreamMetadataTable metaTable = tables.getDataStreamMetadataTable(t);
        Collection<DataStreamMetadataTable.DataStreamMetadataRow> rows = Lists.newArrayListWithCapacity(cells.size());
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
        DataStreamStore.of(tables).deleteStreams(t, toDelete);
        return false;
    }
}