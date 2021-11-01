/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.performance.benchmarks.table;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.util.crypto.Sha256Hash;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * State class for creating a single Atlas table with one wide row.
 * Unlike {@link WideRowTable}, this table may have columns which were never successfully committed, as well as
 * columns which have committed versions, but have newer uncommitted versions on top.
 */
public abstract class WideRowTableWithAbortedValues extends WideRowTable {
    public static final byte[] DUMMY_VALUE = PtBytes.toBytes("dummy");

    /**
     * Number of columns to write a single committed value to.
     */
    public abstract int getNumColsCommitted();

    /**
     * Number of columns to write a single committed value and then multiple uncommitted values at a higher timestamp
     * to.
     */
    public abstract int getNumColsCommittedAndNewerUncommitted();

    /**
     * Number of columns to write multiple uncommitted values to.
     */
    public abstract int getNumColsUncommitted();

    /**
     * Write this number many uncommitted values on top of columns that were uncommitted, and columns that
     * were committed but should have newer committed values. These values are guaranteed to be written at
     * distinct timestamps greater than timestamps committed values were written at.
     *
     * For example, if we have a column A where we want committed and newer uncommitted values and a column B
     * where we want uncommitted values, and this parameter is set to 3, then column A will have four writes
     * (timestamps T1 < T2 < T3 < T4) and column B will have three (timestamps T5 < T6 < T7).
     * The only timestamp that will actually correspond to a valid commit is T1.
     */
    public abstract int getNumUncommittedValuesPerCell();

    @Override
    public int getNumCols() {
        return getNumColsCommitted() + getNumColsCommittedAndNewerUncommitted() + getNumColsUncommitted();
    }

    public int getNumReadableCols() {
        return getNumColsCommitted() + getNumColsCommittedAndNewerUncommitted();
    }

    @Override
    public abstract boolean isPersistent();

    @Override
    public Map<Cell, Long> getFirstCellAtMaxTimestampAsMap() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public Set<Cell> getFirstCellAsSet() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    protected void storeData() {
        // Write committed values.
        writeCommittedValues();
        writeUncommittedValues();
    }

    private void writeCommittedValues() {
        List<Cell> committedCells = getCells(getNumColsCommitted(), CellType.COMMITTED);
        List<Cell> committedWithNewerUncommitted =
                getCells(getNumColsCommittedAndNewerUncommitted(), CellType.COMMITTED_AND_NEWER_UNCOMMITTED);

        services.getTransactionManager().runTaskThrowOnConflict(txn -> {
            Map<Cell, byte[]> values = new HashMap<>();
            allCellsAtMaxTimestamp = new HashMap<>();
            for (Cell cell : Iterables.concat(committedCells, committedWithNewerUncommitted)) {
                allCellsAtMaxTimestamp.put(cell, Long.MAX_VALUE);
                values.put(cell, DUMMY_VALUE);
            }
            txn.put(this.tableRef, values);
            return null;
        });
    }

    private void writeUncommittedValues() {
        IntStream.range(0, getNumUncommittedValuesPerCell()).forEach(_unused -> writeOneVersionOfUncommittedValues());
    }

    private void writeOneVersionOfUncommittedValues() {
        List<Cell> committedWithNewerUncommitted =
                getCells(getNumColsCommittedAndNewerUncommitted(), CellType.COMMITTED_AND_NEWER_UNCOMMITTED);
        List<Cell> uncommitted = getCells(getNumColsUncommitted(), CellType.UNCOMMITTED);

        Map<Cell, byte[]> values = new HashMap<>();
        for (Cell cell : Iterables.concat(committedWithNewerUncommitted, uncommitted)) {
            values.put(cell, DUMMY_VALUE);
        }

        // Simulate getting a timestamp, writing the values, but not putting into the tx table
        long freshTimestamp =
                services.getTransactionManager().getTimestampService().getFreshTimestamp();
        services.getKeyValueService().multiPut(ImmutableMap.of(tableRef, values), freshTimestamp);
    }

    private List<Cell> getCells(int numCells, CellType cellType) {
        return IntStream.range(0, numCells)
                .boxed()
                .map(index -> cell(index, cellType))
                .collect(Collectors.toList());
    }

    private Cell cell(int index, CellType cellType) {
        return Cell.create(Tables.ROW_BYTES.array(), getColumnName(index, cellType));
    }

    private byte[] getColumnName(int index, CellType cellType) {
        // Prepend a hash of the column name, to ensure an even distribution of the various cell types.
        String prefix = cellType.name();
        byte[] unhashedCellName = (prefix + index).getBytes(StandardCharsets.UTF_8);
        byte[] hash = Sha256Hash.computeHash(unhashedCellName).getBytes();
        return EncodingUtils.add(hash, unhashedCellName);
    }

    private enum CellType {
        COMMITTED,
        COMMITTED_AND_NEWER_UNCOMMITTED,
        UNCOMMITTED
    }
}
