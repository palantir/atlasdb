/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.performance.benchmarks.table;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.palantir.atlasdb.keyvalue.api.Cell;

/**
 * State class for creating a single Atlas table with one wide row.
 * Unlike {@link WideRowTable}, this table may have columns which were never successfully committed, as well as
 * columns which have committed versions, but have newer uncommitted versions on top.
 */
public abstract class WideRowTableWithAbortedValues extends WideRowTable {
    public abstract int getNumColsCommitted();
    public abstract int getNumColsCommittedAndNewerUncommitted();
    public abstract int getNumColsUncommitted();

    public abstract int getNumUncommittedValues();

    public int getNumCols() {
        return getNumColsCommitted() + getNumColsCommittedAndNewerUncommitted() + getNumColsUncommitted();
    }

    public abstract boolean isPersistent();

    @Override
    protected void storeData() {
        services.getTransactionManager().runTaskThrowOnConflict(txn -> {
            Map<Cell, byte[]> values = Maps.newHashMap();
            allCellsAtMaxTimestamp = Maps.newHashMap();
            firstCellAtMaxTimestamp = Maps.newHashMap();
            firstCellAtMaxTimestamp.put(cell(0), Long.MAX_VALUE);
            for (int i = 0; i < getNumCols(); i++) {
                Cell curCell = cell(i);
                values.put(curCell, Ints.toByteArray(i));
                allCellsAtMaxTimestamp.put(curCell, Long.MAX_VALUE);
            }
            txn.put(this.tableRef, values);
            return null;
        });
    }

    private Cell cell(int index) {
        return Cell.create(Tables.ROW_BYTES.array(), ("col_" + index).getBytes(StandardCharsets.UTF_8));
    }

    private enum CellType {
        COMMITTED,
        COMMITTED_AND_NEWER_UNCOMMITTED,
        UNCOMMITTED
    }
}
