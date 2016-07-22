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
package com.palantir.atlasdb.table.api;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.SizedColumnRangeSelection;
import com.palantir.atlasdb.transaction.api.ConstraintCheckable;
import com.palantir.common.base.BatchingVisitable;

/*
 * Each AtlasDbTable should implement this interface.
 */
public interface AtlasDbImmutableTable<ROW, COLUMN_VALUE, ROW_RESULT> extends ConstraintCheckable {
    List<COLUMN_VALUE> getRowColumns(ROW row);
    List<COLUMN_VALUE> getRowColumns(ROW row,
                                     ColumnSelection columnSelection);
    Multimap<ROW, COLUMN_VALUE> getRowsMultimap(Iterable<ROW> rows);
    Multimap<ROW, COLUMN_VALUE> getRowsMultimap(Iterable<ROW> rows,
                                                ColumnSelection columnSelection);
    Multimap<ROW, COLUMN_VALUE> getAsyncRowsMultimap(Iterable<ROW> rows,
                                                     ExecutorService exec);
    Multimap<ROW, COLUMN_VALUE> getAsyncRowsMultimap(Iterable<ROW> rows,
                                                     ColumnSelection columnSelection,
                                                     ExecutorService exec);
    Map<ROW, BatchingVisitable<COLUMN_VALUE>> getRowsColumnRange(Iterable<ROW> rows,
                                                                 SizedColumnRangeSelection columnRangeSelection);

    Iterator<Map.Entry<ROW, COLUMN_VALUE>> getRowsColumnRange(Iterable<ROW> rows,
                                                              ColumnRangeSelection columnRangeSelection,
                                                              int batchHint);
}
