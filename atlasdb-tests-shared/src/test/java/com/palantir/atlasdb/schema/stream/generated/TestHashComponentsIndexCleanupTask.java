package com.palantir.atlasdb.schema.stream.generated;

import java.util.Set;

import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.cleaner.api.OnCleanupTask;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.Transaction;

public class TestHashComponentsIndexCleanupTask implements OnCleanupTask {

    private final StreamTestTableFactory tables;

    public TestHashComponentsIndexCleanupTask(Namespace namespace) {
        tables = StreamTestTableFactory.of(namespace);
    }

    @Override
    public boolean cellsCleanedUp(Transaction t, Set<Cell> cells) {
        TestHashComponentsStreamIdxTable usersIndex = tables.getTestHashComponentsStreamIdxTable(t);
        Set<TestHashComponentsStreamIdxTable.TestHashComponentsStreamIdxRow> rows = Sets.newHashSetWithExpectedSize(cells.size());
        for (Cell cell : cells) {
            rows.add(TestHashComponentsStreamIdxTable.TestHashComponentsStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(cell.getRowName()));
        }
        Multimap<TestHashComponentsStreamIdxTable.TestHashComponentsStreamIdxRow, TestHashComponentsStreamIdxTable.TestHashComponentsStreamIdxColumnValue> rowsInDb = usersIndex.getRowsMultimap(rows);
        Set<Long> toDelete = Sets.newHashSetWithExpectedSize(rows.size() - rowsInDb.keySet().size());
        for (TestHashComponentsStreamIdxTable.TestHashComponentsStreamIdxRow rowToDelete : Sets.difference(rows, rowsInDb.keySet())) {
            toDelete.add(rowToDelete.getId());
        }
        TestHashComponentsStreamStore.of(tables).deleteStreams(t, toDelete);
        return false;
    }
}