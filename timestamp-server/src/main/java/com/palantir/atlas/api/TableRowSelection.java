package com.palantir.atlas.api;

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
        this.tableName = Preconditions.checkNotNull(tableName);
        this.rows = Preconditions.checkNotNull(rows);
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
