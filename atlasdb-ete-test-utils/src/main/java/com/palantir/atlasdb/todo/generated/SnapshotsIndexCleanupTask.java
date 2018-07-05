package com.palantir.atlasdb.todo.generated;

import java.util.Set;

import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.cleaner.api.OnCleanupTask;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.Transaction;

public class SnapshotsIndexCleanupTask implements OnCleanupTask {

    private final TodoSchemaTableFactory tables;

    public SnapshotsIndexCleanupTask(Namespace namespace) {
        tables = TodoSchemaTableFactory.of(namespace);
    }

    @Override
    public boolean cellsCleanedUp(Transaction t, Set<Cell> cells) {
        SnapshotsStreamIdxTable usersIndex = tables.getSnapshotsStreamIdxTable(t);
        Set<SnapshotsStreamIdxTable.SnapshotsStreamIdxRow> rows = Sets.newHashSetWithExpectedSize(cells.size());
        for (Cell cell : cells) {
            rows.add(SnapshotsStreamIdxTable.SnapshotsStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(cell.getRowName()));
        }
        Multimap<SnapshotsStreamIdxTable.SnapshotsStreamIdxRow, SnapshotsStreamIdxTable.SnapshotsStreamIdxColumnValue> rowsInDb = usersIndex.getRowsMultimap(rows);
        Set<Long> toDelete = Sets.newHashSetWithExpectedSize(rows.size() - rowsInDb.keySet().size());
        for (SnapshotsStreamIdxTable.SnapshotsStreamIdxRow rowToDelete : Sets.difference(rows, rowsInDb.keySet())) {
            toDelete.add(rowToDelete.getId());
        }
        SnapshotsStreamStore.of(tables).deleteStreams(t, toDelete);
        return false;
    }
}