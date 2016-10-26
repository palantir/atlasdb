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

package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;

/**
 * Represents a batch to load in {@link com.palantir.atlasdb.keyvalue.api.KeyValueService#getRowsColumnRange(
 * com.palantir.atlasdb.keyvalue.api.TableReference, Iterable, ColumnRangeSelection, int, long)}. The overall iteration
 * order returns all requested columns for the first row, followed by all requested columns for the second row, and so
 * forth. Hence, a single batch consists of some contiguous group of rows to fully load, plus optionally a first row
 * that has a different starting column and optionally a last row where we load a number of columns less than the total.
 */
public final class RowsColumnRangeBatchRequest {
    private final Optional<Map.Entry<byte[], BatchColumnRangeSelection>> partialFirstRow;
    private final ImmutableList<byte[]> rowsToLoadFully;
    private final ColumnRangeSelection columnRangeSelection;
    private final Optional<Map.Entry<byte[], BatchColumnRangeSelection>> partialLastRow;

    private RowsColumnRangeBatchRequest(Optional<Entry<byte[], BatchColumnRangeSelection>> partialFirstRow,
            ImmutableList<byte[]> rowsToLoadFully,
            ColumnRangeSelection columnRangeSelection,
            Optional<Entry<byte[], BatchColumnRangeSelection>> partialLastRow) {
        this.partialFirstRow = partialFirstRow;
        this.rowsToLoadFully = rowsToLoadFully;
        this.columnRangeSelection = columnRangeSelection;
        this.partialLastRow = partialLastRow;
    }

    public boolean hasPartialFirstRow() {
        return partialFirstRow.isPresent();
    }

    public Map.Entry<byte[], BatchColumnRangeSelection> getPartialFirstRow() {
        return partialFirstRow.get();
    }

    public List<byte[]> getRowsToLoadFully() {
        return rowsToLoadFully;
    }

    public ColumnRangeSelection getColumnRangeSelection() {
        return columnRangeSelection;
    }

    public boolean hasPartialLastRow() {
        return partialLastRow.isPresent();
    }

    public Map.Entry<byte[], BatchColumnRangeSelection> getPartialLastRow() {
        return partialLastRow.get();
    }

    public static class Builder {
        private Optional<Map.Entry<byte[], BatchColumnRangeSelection>> partialFirstRow = Optional.empty();
        private final ImmutableList.Builder<byte[]> rowsToLoadFully = ImmutableList.builder();
        private ColumnRangeSelection columnRangeSelection;
        private Optional<Map.Entry<byte[], BatchColumnRangeSelection>> partialLastRow = Optional.empty();

        public Builder partialFirstRow(byte[] row, BatchColumnRangeSelection columnRange) {
            return partialFirstRow(Maps.immutableEntry(row, columnRange));
        }

        public Builder partialFirstRow(Map.Entry<byte[], BatchColumnRangeSelection> row) {
            this.partialFirstRow = Optional.of(row);
            return this;
        }

        public Builder columnRangeSelectionForFullRows(ColumnRangeSelection columnRange) {
            this.columnRangeSelection = columnRange;
            return this;
        }

        public Builder fullRow(byte[] row) {
            this.rowsToLoadFully.add(row);
            return this;
        }

        public Builder fullRows(Iterable<byte[]> rows) {
            this.rowsToLoadFully.addAll(rows);
            return this;
        }

        public Builder partialLastRow(byte[] row, BatchColumnRangeSelection batchColumnRange) {
            return partialLastRow(Maps.immutableEntry(row, batchColumnRange));
        }

        public Builder partialLastRow(Map.Entry<byte[], BatchColumnRangeSelection> row) {
            this.partialLastRow = Optional.of(row);
            return this;
        }

        public RowsColumnRangeBatchRequest build() {
            ImmutableList<byte[]> rows = rowsToLoadFully.build();
            Preconditions.checkState(columnRangeSelection != null || rows.isEmpty(),
                                     "Must specify a column range selection when loading full rows.");
            return new RowsColumnRangeBatchRequest(partialFirstRow, rows, columnRangeSelection, partialLastRow);
        }
    }
}
