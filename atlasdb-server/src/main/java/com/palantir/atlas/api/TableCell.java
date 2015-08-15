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
package com.palantir.atlas.api;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.keyvalue.api.Cell;


/**
 * For tables with named columns,
 * <pre>
 * {
 *   "table": &lt;tableName>
 *   "data": [
 *     {
 *       "row": [&lt;component>, ...],
 *       "col": &lt;short col name>
 *     },
 *     ...
 *   ]
 * }
 * </pre>
 * <p>
 * For tables with dynamic columns,
 * <pre>
 * {
 *   "table": &lt;tableName>
 *   "data": [
 *     {
 *       "row": [&lt;component>, ...],
 *       "col": [&lt;component>, ...]
 *     },
 *     ...
 *   ]
 * }
 * </pre>
 */
public class TableCell {
    private final String tableName;
    private final Iterable<Cell> cells;

    public TableCell(String tableName, Iterable<Cell> cells) {
        this.tableName = Preconditions.checkNotNull(tableName);
        this.cells = Preconditions.checkNotNull(cells);
    }

    public String getTableName() {
        return tableName;
    }

    public Iterable<Cell> getCells() {
        return cells;
    }

    @Override
    public String toString() {
        return "TableCell [tableName=" + tableName + ", cells=" + cells + "]";
    }
}
