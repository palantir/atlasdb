package com.palantir.atlasdb.schema.stream.generated;

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

public class StreamTestWithHashMetadataCleanupTask implements OnCleanupTask {

    private final StreamTestTableFactory tables;

    public StreamTestWithHashMetadataCleanupTask(Namespace namespace) {
        tables = StreamTestTableFactory.of(namespace);
    }

    @Override
    public boolean cellsCleanedUp(Transaction t, Set<Cell> cells) {
        StreamTestWithHashStreamMetadataTable metaTable = tables.getStreamTestWithHashStreamMetadataTable(t);
        Collection<StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow> rows = Lists.newArrayListWithCapacity(cells.size());
        for (Cell cell : cells) {
            rows.add(StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow.BYTES_HYDRATOR.hydrateFromBytes(cell.getRowName()));
        }
        Map<StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow, StreamMetadata> currentMetadata = metaTable.getMetadatas(rows);
        Set<Long> toDelete = Sets.newHashSet();
        for (Map.Entry<StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow, StreamMetadata> e : currentMetadata.entrySet()) {
            if (e.getValue().getStatus() != Status.STORED) {
                toDelete.add(e.getKey().getId());
            }
        }
        StreamTestWithHashStreamStore.of(tables).deleteStreams(t, toDelete);
        return false;
    }
}