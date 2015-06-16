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

package com.palantir.atlasdb.table.api;

import java.util.List;
import java.util.concurrent.ExecutorService;

import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.transaction.api.ConstraintCheckable;

/*
 * Each AtlasDbTable should implement this interface.
 */
public interface AtlasDbImmutableTable<ROW, COLUMN_VALUE, ROW_RESULT> extends ConstraintCheckable {
    public List<COLUMN_VALUE> getRowColumns(ROW row);
    public List<COLUMN_VALUE> getRowColumns(ROW row,
                                            ColumnSelection columnSelection);
    public Multimap<ROW, COLUMN_VALUE> getRowsMultimap(Iterable<ROW> rows);
    public Multimap<ROW, COLUMN_VALUE> getRowsMultimap(Iterable<ROW> rows,
                                                       ColumnSelection columnSelection);
    public Multimap<ROW, COLUMN_VALUE> getAsyncRowsMultimap(Iterable<ROW> rows,
                                                            ExecutorService exec);
    public Multimap<ROW, COLUMN_VALUE> getAsyncRowsMultimap(Iterable<ROW> rows,
                                                            ColumnSelection columnSelection,
                                                            ExecutorService exec);
}
