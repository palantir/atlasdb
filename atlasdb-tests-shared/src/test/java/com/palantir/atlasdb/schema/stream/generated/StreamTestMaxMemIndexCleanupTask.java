package com.palantir.atlasdb.schema.stream.generated;

import java.util.Set;

import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.cleaner.api.OnCleanupTask;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.Transaction;

public class StreamTestMaxMemIndexCleanupTask implements OnCleanupTask {

    private final StreamTestTableFactory tables;

    public StreamTestMaxMemIndexCleanupTask(Namespace namespace) {
        tables = StreamTestTableFactory.of(namespace);
    }

    @Override
    public boolean cellsCleanedUp(Transaction t, Set<Cell> cells) {
        StreamTestMaxMemStreamIdxTable usersIndex = tables.getStreamTestMaxMemStreamIdxTable(t);
        Set<StreamTestMaxMemStreamIdxTable.StreamTestMaxMemStreamIdxRow> rows = Sets.newHashSetWithExpectedSize(cells.size());
        for (Cell cell : cells) {
            rows.add(StreamTestMaxMemStreamIdxTable.StreamTestMaxMemStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(cell.getRowName()));
        }
        Multimap<StreamTestMaxMemStreamIdxTable.StreamTestMaxMemStreamIdxRow, StreamTestMaxMemStreamIdxTable.StreamTestMaxMemStreamIdxColumnValue> rowsInDb = usersIndex.getRowsMultimap(rows);
        Set<Long> toDelete = Sets.newHashSetWithExpectedSize(rows.size() - rowsInDb.keySet().size());
        for (StreamTestMaxMemStreamIdxTable.StreamTestMaxMemStreamIdxRow rowToDelete : Sets.difference(rows, rowsInDb.keySet())) {
            toDelete.add(rowToDelete.getId());
        }
        StreamTestMaxMemStreamStore.of(tables).deleteStreams(t, toDelete);
        return false;
    }
}