package com.palantir.example.profile.schema.generated;

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

public class UserPhotosMetadataCleanupTask implements OnCleanupTask {

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
        Map<UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow, StreamMetadata> currentMetadata = metaTable.getMetadatas(rows);
        Set<Long> toDelete = Sets.newHashSet();
        for (Map.Entry<UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow, StreamMetadata> e : currentMetadata.entrySet()) {
            if (e.getValue().getStatus() != Status.STORED) {
                toDelete.add(e.getKey().getId());
            }
        }
        UserPhotosStreamStore.of(tables).deleteStreams(t, toDelete);
        return false;
    }
}