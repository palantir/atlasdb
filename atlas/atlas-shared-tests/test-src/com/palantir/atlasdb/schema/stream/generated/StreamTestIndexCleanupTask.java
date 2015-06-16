// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.schema.stream.generated;

import java.util.Set;

import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.cleaner.api.OnCleanupTask;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.Transaction;

public class StreamTestIndexCleanupTask implements OnCleanupTask {

    private final StreamTestTableFactory tables = StreamTestTableFactory.of();

    @Override
    public boolean cellsCleanedUp(Transaction t, Set<Cell> cells) {
        StreamTestStreamIdxTable usersIndex = tables.getStreamTestStreamIdxTable(t);
        Set<StreamTestStreamIdxTable.StreamTestStreamIdxRow> rows = Sets.newHashSetWithExpectedSize(cells.size());
        for (Cell cell : cells) {
            rows.add(StreamTestStreamIdxTable.StreamTestStreamIdxRow.of((Long) ValueType.VAR_LONG.convertToJava(cell.getRowName(), 0)));
        }
        Multimap<StreamTestStreamIdxTable.StreamTestStreamIdxRow, StreamTestStreamIdxTable.StreamTestStreamIdxColumnValue> rowsInDb = usersIndex.getRowsMultimap(rows);
        Set<Long> toDelete = Sets.newHashSetWithExpectedSize(rows.size() - rowsInDb.keySet().size());
        for (StreamTestStreamIdxTable.StreamTestStreamIdxRow rowToDelete : Sets.difference(rows, rowsInDb.keySet())) {
            toDelete.add(rowToDelete.getId());
        }
        StreamTestStreamStore.of(t, tables).deleteStreams(toDelete);
        return false;
    }
}