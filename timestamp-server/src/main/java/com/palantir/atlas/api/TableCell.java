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
