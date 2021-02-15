package com.palantir.atlasdb.todo.generated;

import java.util.Map;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Sets;
import com.palantir.atlasdb.cleaner.api.OnCleanupTask;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.common.base.BatchingVisitable;

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
        BatchColumnRangeSelection oneColumn = BatchColumnRangeSelection.create(
                PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1);
        Map<SnapshotsStreamIdxTable.SnapshotsStreamIdxRow, BatchingVisitable<SnapshotsStreamIdxTable.SnapshotsStreamIdxColumnValue>> existentRows
                = usersIndex.getRowsColumnRange(rows, oneColumn);
        Set<SnapshotsStreamIdxTable.SnapshotsStreamIdxRow> rowsInDb = Sets.newHashSetWithExpectedSize(cells.size());
        for (Map.Entry<SnapshotsStreamIdxTable.SnapshotsStreamIdxRow, BatchingVisitable<SnapshotsStreamIdxTable.SnapshotsStreamIdxColumnValue>> rowVisitable
                : existentRows.entrySet()) {
            rowVisitable.getValue().batchAccept(1, columnValues -> {
                if (!columnValues.isEmpty()) {
                    rowsInDb.add(rowVisitable.getKey());
                }
                return false;
            });
        }
        Set<Long> toDelete = Sets.newHashSetWithExpectedSize(rows.size() - rowsInDb.size());
        for (SnapshotsStreamIdxTable.SnapshotsStreamIdxRow rowToDelete : Sets.difference(rows, rowsInDb)) {
            toDelete.add(rowToDelete.getId());
        }
        SnapshotsStreamStore.of(tables).deleteStreams(t, toDelete);
        return false;
    }
}