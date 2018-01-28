package com.palantir.atlasdb.blob.generated;

import java.util.Set;

import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.cleaner.api.OnCleanupTask;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.Transaction;

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
        Multimap<HotspottyDataStreamIdxTable.HotspottyDataStreamIdxRow, HotspottyDataStreamIdxTable.HotspottyDataStreamIdxColumnValue> rowsInDb = usersIndex.getRowsMultimap(rows);
        Set<Long> toDelete = Sets.newHashSetWithExpectedSize(rows.size() - rowsInDb.keySet().size());
        for (HotspottyDataStreamIdxTable.HotspottyDataStreamIdxRow rowToDelete : Sets.difference(rows, rowsInDb.keySet())) {
            toDelete.add(rowToDelete.getId());
        }
        HotspottyDataStreamStore.of(tables).deleteStreams(t, toDelete);
        return false;
    }
}