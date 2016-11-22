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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.common.base.ClosableIterator;
import com.palantir.nexus.db.sql.AgnosticLightResultRow;

public interface DbReadTable {
    ClosableIterator<AgnosticLightResultRow> getLatestRows(
            Iterable<byte[]> rows, ColumnSelection columns, long ts, boolean includeValue);
    ClosableIterator<AgnosticLightResultRow> getLatestRows(
            Map<byte[], Long> rows, ColumnSelection columns, boolean includeValue);
    ClosableIterator<AgnosticLightResultRow> getAllRows(
            Iterable<byte[]> rows, ColumnSelection columns, long ts, boolean includeValue);
    ClosableIterator<AgnosticLightResultRow> getAllRows(
            Map<byte[], Long> rows, ColumnSelection columns, boolean includeValue);

    ClosableIterator<AgnosticLightResultRow> getLatestCells(Iterable<Cell> cells, long ts, boolean includeValue);
    ClosableIterator<AgnosticLightResultRow> getLatestCells(Map<Cell, Long> cells, boolean includeValue);
    ClosableIterator<AgnosticLightResultRow> getAllCells(Iterable<Cell> cells, long ts, boolean includeValue);
    ClosableIterator<AgnosticLightResultRow> getAllCells(Map<Cell, Long> cells, boolean includeValue);
    ClosableIterator<AgnosticLightResultRow> getRange(RangeRequest range, long ts, int maxRows);

    ClosableIterator<AgnosticLightResultRow> getRowsColumnRangeCounts(
            List<byte[]> rows,
            long ts,
            ColumnRangeSelection columnRangeSelection);

    ClosableIterator<AgnosticLightResultRow> getRowsColumnRange(
            Map<byte[], BatchColumnRangeSelection> columnRangeSelectionsByRow,
            long ts);
    ClosableIterator<AgnosticLightResultRow> getRowsColumnRange(
            RowsColumnRangeBatchRequest rowsColumnRangeBatch,
            long ts);

    boolean hasOverflowValues();
    ClosableIterator<AgnosticLightResultRow> getOverflow(Collection<OverflowValue> overflowIds);
}
