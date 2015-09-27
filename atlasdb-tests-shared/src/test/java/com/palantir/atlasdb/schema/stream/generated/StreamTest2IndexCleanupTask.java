package com.palantir.atlasdb.schema.stream.generated;

import java.util.Set;

import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.cleaner.api.OnCleanupTask;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.Transaction;

public class StreamTest2IndexCleanupTask implements OnCleanupTask {

    private final StreamTestTableFactory tables = StreamTestTableFactory.of();

    @Override
    public boolean cellsCleanedUp(Transaction t, Set<Cell> cells) {
        StreamTest2StreamIdxTable usersIndex = tables.getStreamTest2StreamIdxTable(t);
        Set<StreamTest2StreamIdxTable.StreamTest2StreamIdxRow> rows = Sets.newHashSetWithExpectedSize(cells.size());
        for (Cell cell : cells) {
            rows.add(StreamTest2StreamIdxTable.StreamTest2StreamIdxRow.of((Long) ValueType.VAR_LONG.convertToJava(cell.getRowName(), 0)));
        }
        Multimap<StreamTest2StreamIdxTable.StreamTest2StreamIdxRow, StreamTest2StreamIdxTable.StreamTest2StreamIdxColumnValue> rowsInDb = usersIndex.getRowsMultimap(rows);
        Set<Long> toDelete = Sets.newHashSetWithExpectedSize(rows.size() - rowsInDb.keySet().size());
        for (StreamTest2StreamIdxTable.StreamTest2StreamIdxRow rowToDelete : Sets.difference(rows, rowsInDb.keySet())) {
            toDelete.add(rowToDelete.getId());
        }
        StreamTest2StreamStore.of(tables).deleteStreams(t, toDelete);
        return false;
    }
}