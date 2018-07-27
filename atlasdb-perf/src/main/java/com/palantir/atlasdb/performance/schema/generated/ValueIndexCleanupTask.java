package com.palantir.atlasdb.performance.schema.generated;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import com.palantir.atlasdb.cleaner.api.OnCleanupTask;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.performance.schema.generated.ValueStreamIdxTable.ValueStreamIdxColumnValue;
import com.palantir.atlasdb.performance.schema.generated.ValueStreamIdxTable.ValueStreamIdxRow;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.common.base.BatchingVisitable;

public class ValueIndexCleanupTask implements OnCleanupTask {
    private static final BatchColumnRangeSelection ONE_COLUMN = BatchColumnRangeSelection.create(
            PtBytes.EMPTY_BYTE_ARRAY,
            PtBytes.EMPTY_BYTE_ARRAY,
            1);

    private final StreamTestTableFactory tables;

    public ValueIndexCleanupTask(Namespace namespace) {
        tables = StreamTestTableFactory.of(namespace);
    }

    @Override
    public boolean cellsCleanedUp(Transaction t, Set<Cell> cells) {
        ValueStreamIdxTable usersIndex = tables.getValueStreamIdxTable(t);
        Set<ValueStreamIdxTable.ValueStreamIdxRow> rows = Sets.newHashSetWithExpectedSize(cells.size());
        for (Cell cell : cells) {
            rows.add(ValueStreamIdxTable.ValueStreamIdxRow.of((Long) ValueType.VAR_LONG.convertToJava(cell.getRowName(), 0)));
        }

        Map<ValueStreamIdxRow, BatchingVisitable<ValueStreamIdxColumnValue>> presentRowsMultimap
                = usersIndex.getRowsColumnRange(rows, ONE_COLUMN);
        Set<ValueStreamIdxRow> presentRows = Sets.newHashSetWithExpectedSize(cells.size());
        for (Map.Entry<ValueStreamIdxRow, BatchingVisitable<ValueStreamIdxColumnValue>> rowVisitable
                : presentRowsMultimap.entrySet()) {
            rowVisitable.getValue().batchAccept(1, columnValue -> {
                if (!columnValue.isEmpty()) {
                    presentRows.add(rowVisitable.getKey());
                }
                return false;
            });
        }

        // Loads all refs, which seems unnecessary!
//        Multimap<ValueStreamIdxTable.ValueStreamIdxRow, ValueStreamIdxTable.ValueStreamIdxColumnValue> rowsInDb = usersIndex.getRowsMultimap(rows);
//        Set<Long> toDelete = Sets.newHashSetWithExpectedSize(rows.size() - rowsInDb.keySet().size());
//        for (ValueStreamIdxTable.ValueStreamIdxRow rowToDelete : Sets.difference(rows, rowsInDb.keySet())) {
//            toDelete.add(rowToDelete.getId());

        Set<Long> toDelete = Sets.newHashSetWithExpectedSize(rows.size() - presentRows.size());
        for (ValueStreamIdxTable.ValueStreamIdxRow rowToDelete : Sets.difference(rows, presentRows)) {
            toDelete.add(rowToDelete.getId());
        }
        ValueStreamStore.of(tables).deleteStreams(t, toDelete);
        return false;
    }
}
