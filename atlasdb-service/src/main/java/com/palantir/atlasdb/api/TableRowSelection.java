/*
 * (c) Copyright 2015 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.api;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;

/**
 * <pre>
 * {
 *   "table": &lt;tableName>,
 *   "rows": [
 *     [&lt;component>, ...],
 *     ...
 *   ]
 *   "cols": [&lt;short col name>, ...] (default all columns)
 * }
 * </pre>
 */
public class TableRowSelection {
    private final String tableName;
    private final Iterable<byte[]> rows;
    private final ColumnSelection columnSelection;

    public TableRowSelection(String tableName, Iterable<byte[]> rows, ColumnSelection columnSelection) {
        this.tableName = Preconditions.checkNotNull(tableName, "tableName must not be null!");
        this.rows = Preconditions.checkNotNull(rows, "rows must not be null!");
        this.columnSelection = columnSelection;
    }

    public String getTableName() {
        return tableName;
    }

    public Iterable<byte[]> getRows() {
        return rows;
    }

    public ColumnSelection getColumnSelection() {
        return columnSelection;
    }

    @Override
    public String toString() {
        return "TableRow [tableName=" + tableName + ", rows=" + rows + ", columnSelection="
                + columnSelection + "]";
    }
}
