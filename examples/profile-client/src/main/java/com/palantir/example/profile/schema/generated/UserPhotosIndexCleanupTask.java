/**
 * Copyright 2015 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.example.profile.schema.generated;

import java.util.Set;

import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.cleaner.api.OnCleanupTask;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.Transaction;

public class UserPhotosIndexCleanupTask implements OnCleanupTask {

    private final ProfileTableFactory tables = ProfileTableFactory.of();

    @Override
    public boolean cellsCleanedUp(Transaction t, Set<Cell> cells) {
        UserPhotosStreamIdxTable usersIndex = tables.getUserPhotosStreamIdxTable(t);
        Set<UserPhotosStreamIdxTable.UserPhotosStreamIdxRow> rows = Sets.newHashSetWithExpectedSize(cells.size());
        for (Cell cell : cells) {
            rows.add(UserPhotosStreamIdxTable.UserPhotosStreamIdxRow.of((Long) ValueType.VAR_LONG.convertToJava(cell.getRowName(), 0)));
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
