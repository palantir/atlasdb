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
package com.palantir.atlasdb.table.api;

import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.transaction.api.ConstraintCheckable;
import com.palantir.common.base.BatchingVisitable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/*
 * Each AtlasDbTable should implement this interface.
 */
public interface AtlasDbImmutableTable<ROW, COLUMN_VALUE, ROW_RESULT> extends ConstraintCheckable {
    List<COLUMN_VALUE> getRowColumns(ROW row);

    List<COLUMN_VALUE> getRowColumns(ROW row, ColumnSelection columnSelection);

    Multimap<ROW, COLUMN_VALUE> getRowsMultimap(Iterable<ROW> rows);

    Multimap<ROW, COLUMN_VALUE> getRowsMultimap(Iterable<ROW> rows, ColumnSelection columnSelection);

    /*
     * This returns visitables for each row. It should be used if you want to visit a number of the first
     * matched columns instead of all the columns per row.
     */
    Map<ROW, BatchingVisitable<COLUMN_VALUE>> getRowsColumnRange(
            Iterable<ROW> rows, BatchColumnRangeSelection columnRangeSelection);

    /*
     * This returns an iterator for each row. It should be used if you want to visit a number of the first
     * matched columns instead of all the columns per row.
     */
    Map<ROW, Iterator<COLUMN_VALUE>> getRowsColumnRangeIterator(
            Iterable<ROW> rows, BatchColumnRangeSelection columnRangeSelection);

    /*
     * This returns an iterator that visits the result columns row by row.
     *
     * @param rows rows to get
     * @param columnRangeSelection column range selection for each row
     * @param batchHint batch size for reading from the database
     */
    Iterator<Map.Entry<ROW, COLUMN_VALUE>> getRowsColumnRange(
            Iterable<ROW> rows, ColumnRangeSelection columnRangeSelection, int batchHint);
}
