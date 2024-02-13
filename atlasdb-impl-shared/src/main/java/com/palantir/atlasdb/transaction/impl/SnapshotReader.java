/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl;

import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.base.BatchingVisitable;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;

/**
 * This will load the given keys from the underlying key value service and apply postFiltering so we have snapshot
 * isolation.  If the value in the key value service is the empty array this will be included here and needs to be
 * filtered out.
 */
public interface SnapshotReader {
    Map<Cell, byte[]> get(TableReference tableRef, Set<Cell> cells);

    NavigableMap<byte[], RowResult<byte[]>> getRows(
            TableReference tableRef, Iterable<byte[]> rows, ColumnSelection columnSelection);

    BatchingVisitable<RowResult<byte[]>> getRange(TableReference tableRef, RangeRequest rangeRequest);

    Map<byte[], BatchingVisitable<Entry<Cell, byte[]>>> getRowsColumnRange(
            TableReference tableRef, Iterable<byte[]> rows, BatchColumnRangeSelection columnRangeSelection);

    Iterator<Entry<Cell, byte[]>> getSortedColumns(
            TableReference tableRef, Iterable<byte[]> rows, BatchColumnRangeSelection batchColumnRangeSelection);
}
