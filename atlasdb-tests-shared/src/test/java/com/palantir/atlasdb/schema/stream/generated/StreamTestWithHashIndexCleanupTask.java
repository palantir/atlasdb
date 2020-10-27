package com.palantir.atlasdb.schema.stream.generated;

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

public class StreamTestWithHashIndexCleanupTask implements OnCleanupTask {

    private final StreamTestTableFactory tables;

    public StreamTestWithHashIndexCleanupTask(Namespace namespace) {
        tables = StreamTestTableFactory.of(namespace);
    }

    @Override
    public boolean cellsCleanedUp(Transaction t, Set<Cell> cells) {
        StreamTestWithHashStreamIdxTable usersIndex = tables.getStreamTestWithHashStreamIdxTable(t);
        Set<StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxRow> rows = Sets.newHashSetWithExpectedSize(cells.size());
        for (Cell cell : cells) {
            rows.add(StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(cell.getRowName()));
        }
        BatchColumnRangeSelection oneColumn = BatchColumnRangeSelection.create(
                PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1);
        Map<StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxRow, BatchingVisitable<StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxColumnValue>> existentRows
                = usersIndex.getRowsColumnRange(rows, oneColumn);
        Set<StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxRow> rowsInDb = Sets.newHashSetWithExpectedSize(cells.size());
        for (Map.Entry<StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxRow, BatchingVisitable<StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxColumnValue>> rowVisitable
                : existentRows.entrySet()) {
            rowVisitable.getValue().batchAccept(1, columnValues -> {
                if (!columnValues.isEmpty()) {
                    rowsInDb.add(rowVisitable.getKey());
                }
                return false;
            });
        }
        Set<Long> toDelete = Sets.newHashSetWithExpectedSize(rows.size() - rowsInDb.size());
        for (StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxRow rowToDelete : Sets.difference(rows, rowsInDb)) {
            toDelete.add(rowToDelete.getId());
        }
        StreamTestWithHashStreamStore.of(tables).deleteStreams(t, toDelete);
        return false;
    }
}