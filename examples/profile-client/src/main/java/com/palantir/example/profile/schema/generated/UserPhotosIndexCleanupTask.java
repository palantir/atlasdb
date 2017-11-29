package com.palantir.example.profile.schema.generated;

import java.util.Set;

import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.cleaner.api.OnCleanupTask;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.Transaction;

public class UserPhotosIndexCleanupTask implements OnCleanupTask {

    private final ProfileTableFactory tables;

    public UserPhotosIndexCleanupTask(Namespace namespace) {
        tables = ProfileTableFactory.of(namespace);
    }

    @Override
    public boolean cellsCleanedUp(Transaction t, Set<Cell> cells) {
        UserPhotosStreamIdxTable usersIndex = tables.getUserPhotosStreamIdxTable(t);
        Set<UserPhotosStreamIdxTable.UserPhotosStreamIdxRow> rows = Sets.newHashSetWithExpectedSize(cells.size());
        for (Cell cell : cells) {
            rows.add(UserPhotosStreamIdxTable.UserPhotosStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(cell.getRowName()));
        }
        Multimap<UserPhotosStreamIdxTable.UserPhotosStreamIdxRow, UserPhotosStreamIdxTable.UserPhotosStreamIdxColumnValue> rowsInDb = usersIndex.getRowsMultimap(rows);
        Set<Long> toDelete = Sets.newHashSetWithExpectedSize(rows.size() - rowsInDb.keySet().size());
        for (UserPhotosStreamIdxTable.UserPhotosStreamIdxRow rowToDelete : Sets.difference(rows, rowsInDb.keySet())) {
            toDelete.add(rowToDelete.getId());
        }
        UserPhotosStreamStore.of(tables).deleteStreams(t, toDelete);
        return false;
    }
}