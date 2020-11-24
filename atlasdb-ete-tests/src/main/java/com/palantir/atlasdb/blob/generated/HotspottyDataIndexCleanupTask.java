package com.palantir.atlasdb.blob.generated;

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

public class HotspottyDataIndexCleanupTask implements OnCleanupTask {

    private final BlobSchemaTableFactory tables;

    public HotspottyDataIndexCleanupTask(Namespace namespace) {
        tables = BlobSchemaTableFactory.of(namespace);
    }

    @Override
    public boolean cellsCleanedUp(Transaction t, Set<Cell> cells) {
        HotspottyDataStreamIdxTable usersIndex = tables.getHotspottyDataStreamIdxTable(t);
        Set<HotspottyDataStreamIdxTable.HotspottyDataStreamIdxRow> rows = Sets.newHashSetWithExpectedSize(cells.size());
        for (Cell cell : cells) {
            rows.add(HotspottyDataStreamIdxTable.HotspottyDataStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(cell.getRowName()));
        }
        BatchColumnRangeSelection oneColumn = BatchColumnRangeSelection.create(
                PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1);
        Map<HotspottyDataStreamIdxTable.HotspottyDataStreamIdxRow, BatchingVisitable<HotspottyDataStreamIdxTable.HotspottyDataStreamIdxColumnValue>> existentRows
                = usersIndex.getRowsColumnRange(rows, oneColumn);
        Set<HotspottyDataStreamIdxTable.HotspottyDataStreamIdxRow> rowsInDb = Sets.newHashSetWithExpectedSize(cells.size());
        for (Map.Entry<HotspottyDataStreamIdxTable.HotspottyDataStreamIdxRow, BatchingVisitable<HotspottyDataStreamIdxTable.HotspottyDataStreamIdxColumnValue>> rowVisitable
                : existentRows.entrySet()) {
            rowVisitable.getValue().batchAccept(1, columnValues -> {
                if (!columnValues.isEmpty()) {
                    rowsInDb.add(rowVisitable.getKey());
                }
                return false;
            });
        }
        Set<Long> toDelete = Sets.newHashSetWithExpectedSize(rows.size() - rowsInDb.size());
        for (HotspottyDataStreamIdxTable.HotspottyDataStreamIdxRow rowToDelete : Sets.difference(rows, rowsInDb)) {
            toDelete.add(rowToDelete.getId());
        }
        HotspottyDataStreamStore.of(tables).deleteStreams(t, toDelete);
        return false;
    }
}