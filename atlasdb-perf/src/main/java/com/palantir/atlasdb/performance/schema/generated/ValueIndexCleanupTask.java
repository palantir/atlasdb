package com.palantir.atlasdb.performance.schema.generated;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import com.palantir.atlasdb.cleaner.api.OnCleanupTask;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.common.base.BatchingVisitable;

public class ValueIndexCleanupTask implements OnCleanupTask {

    private final StreamTestTableFactory tables;

    public ValueIndexCleanupTask(Namespace namespace) {
        tables = StreamTestTableFactory.of(namespace);
    }

    @Override
    public boolean cellsCleanedUp(Transaction t, Set<Cell> cells) {
        ValueStreamIdxTable usersIndex = tables.getValueStreamIdxTable(t);
        Set<ValueStreamIdxTable.ValueStreamIdxRow> rows = Sets.newHashSetWithExpectedSize(cells.size());
        for (Cell cell : cells) {
            rows.add(ValueStreamIdxTable.ValueStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(cell.getRowName()));
        }
        BatchColumnRangeSelection oneColumn = BatchColumnRangeSelection.create(
                PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1);
        Map<ValueStreamIdxTable.ValueStreamIdxRow, BatchingVisitable<ValueStreamIdxTable.ValueStreamIdxColumnValue>> existentRows
                = usersIndex.getRowsColumnRange(rows, oneColumn);
        Set<ValueStreamIdxTable.ValueStreamIdxRow> rowsInDb = Sets.newHashSetWithExpectedSize(cells.size());
        for (Map.Entry<ValueStreamIdxTable.ValueStreamIdxRow, BatchingVisitable<ValueStreamIdxTable.ValueStreamIdxColumnValue>> rowVisitable
                : existentRows.entrySet()) {
            rowVisitable.getValue().batchAccept(1, columnValues -> {
                if (!columnValues.isEmpty()) {
                    rowsInDb.add(rowVisitable.getKey());
                }
                return false;
            });
        }
        Set<Long> toDelete = Sets.newHashSetWithExpectedSize(rows.size() - rowsInDb.size());
        for (ValueStreamIdxTable.ValueStreamIdxRow rowToDelete : Sets.difference(rows, rowsInDb)) {
            toDelete.add(rowToDelete.getId());
        }
        ValueStreamStore.of(tables).deleteStreams(t, toDelete);
        return false;
    }
}