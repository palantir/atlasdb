package com.palantir.atlasdb.schema.stream.generated;

import java.util.Set;

import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.cleaner.api.OnCleanupTask;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.Transaction;

public class StreamTestIndexCleanupTask implements OnCleanupTask {

    private final StreamTestTableFactory tables;

    public StreamTestIndexCleanupTask(Namespace namespace) {
        tables = StreamTestTableFactory.of(namespace);
    }

    @Override
    public boolean cellsCleanedUp(Transaction t, Set<Cell> cells) {
        StreamTestStreamIdxTable usersIndex = tables.getStreamTestStreamIdxTable(t);
        Set<StreamTestStreamIdxTable.StreamTestStreamIdxRow> rows = Sets.newHashSetWithExpectedSize(cells.size());
        for (Cell cell : cells) {
            rows.add(StreamTestStreamIdxTable.StreamTestStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(cell.getRowName()));
        }
        Multimap<StreamTestStreamIdxTable.StreamTestStreamIdxRow, StreamTestStreamIdxTable.StreamTestStreamIdxColumnValue> rowsInDb = usersIndex.getRowsMultimap(rows);
        Set<Long> toDelete = Sets.newHashSetWithExpectedSize(rows.size() - rowsInDb.keySet().size());
        for (StreamTestStreamIdxTable.StreamTestStreamIdxRow rowToDelete : Sets.difference(rows, rowsInDb.keySet())) {
            toDelete.add(rowToDelete.getId());
        }
        StreamTestStreamStore.of(tables).deleteStreams(t, toDelete);
        return false;
    }
}