package com.palantir.atlasdb.performance.schema.generated;

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

public class ValueMetadataCleanupTask implements OnCleanupTask {

    private final StreamTestTableFactory tables;

    public ValueMetadataCleanupTask(Namespace namespace) {
        tables = StreamTestTableFactory.of(namespace);
    }

    @Override
    public boolean cellsCleanedUp(Transaction t, Set<Cell> cells) {
        ValueStreamMetadataTable metaTable = tables.getValueStreamMetadataTable(t);
        Collection<ValueStreamMetadataTable.ValueStreamMetadataRow> rows = Lists.newArrayListWithCapacity(cells.size());
        for (Cell cell : cells) {
            rows.add(ValueStreamMetadataTable.ValueStreamMetadataRow.BYTES_HYDRATOR.hydrateFromBytes(cell.getRowName()));
        }
        Map<ValueStreamMetadataTable.ValueStreamMetadataRow, StreamMetadata> currentMetadata = metaTable.getMetadatas(rows);
        Set<Long> toDelete = Sets.newHashSet();
        for (Map.Entry<ValueStreamMetadataTable.ValueStreamMetadataRow, StreamMetadata> e : currentMetadata.entrySet()) {
            if (e.getValue().getStatus() != Status.STORED) {
                toDelete.add(e.getKey().getId());
            }
        }
        ValueStreamStore.of(tables).deleteStreams(t, toDelete);
        return false;
    }
}