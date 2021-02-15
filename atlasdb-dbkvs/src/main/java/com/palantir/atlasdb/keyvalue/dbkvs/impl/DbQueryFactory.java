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
package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import java.util.Collection;
import java.util.Map;

public interface DbQueryFactory {
    FullQuery getLatestRowQuery(byte[] row, long ts, ColumnSelection columns, boolean includeValue);

    FullQuery getLatestRowsQuery(Iterable<byte[]> rows, long ts, ColumnSelection columns, boolean includeValue);

    FullQuery getLatestRowsQuery(
            Collection<Map.Entry<byte[], Long>> rows, ColumnSelection columns, boolean includeValue);

    FullQuery getAllRowQuery(byte[] row, long ts, ColumnSelection columns, boolean includeValue);

    FullQuery getAllRowsQuery(Iterable<byte[]> rows, long ts, ColumnSelection columns, boolean includeValue);

    FullQuery getAllRowsQuery(Collection<Map.Entry<byte[], Long>> rows, ColumnSelection columns, boolean includeValue);

    FullQuery getLatestCellQuery(Cell cell, long ts, boolean includeValue);

    FullQuery getLatestCellsQuery(Iterable<Cell> cells, long ts, boolean includeValue);

    FullQuery getLatestCellsQuery(Collection<Map.Entry<Cell, Long>> cells, boolean includeValue);

    FullQuery getAllCellQuery(Cell cell, long ts, boolean includeValue);

    FullQuery getAllCellsQuery(Iterable<Cell> cells, long ts, boolean includeValue);

    FullQuery getAllCellsQuery(Collection<Map.Entry<Cell, Long>> cells, boolean includeValue);

    FullQuery getRangeQuery(RangeRequest range, long ts, int maxRows);

    boolean hasOverflowValues();

    FullQuery getRowsColumnRangeCountsQuery(Iterable<byte[]> rows, long ts, ColumnRangeSelection columnRangeSelection);

    FullQuery getRowsColumnRangeQuery(Map<byte[], BatchColumnRangeSelection> columnRangeSelectionsByRow, long ts);

    FullQuery getRowsColumnRangeQuery(RowsColumnRangeBatchRequest batch, long ts);
}
