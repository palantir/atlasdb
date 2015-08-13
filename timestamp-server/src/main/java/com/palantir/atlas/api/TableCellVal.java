package com.palantir.atlas.api;

import java.util.Map;

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
 *       "&lt;short col name>": &lt;value>
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
 *       "col": [&lt;component>, ...],
 *       "val": &lt;value>
 *     },
 *     ...
 *   ]
 * }
 * </pre>
 */
public class TableCellVal {
    private final String tableName;
    private final Map<Cell, byte[]> results;

    public TableCellVal(String tableName, Map<Cell, byte[]> results) {
        this.tableName = Preconditions.checkNotNull(tableName);
        this.results = Preconditions.checkNotNull(results);
    }

    public String getTableName() {
        return tableName;
    }

    public Map<Cell, byte[]> getResults() {
        return results;
    }

    @Override
    public String toString() {
        return "TableCellVal [tableName=" + tableName + ", results=" + results + "]";
    }
}
