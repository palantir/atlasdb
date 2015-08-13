package com.palantir.atlas.api;

import java.util.Collection;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.keyvalue.api.RowResult;

/**
 * For tables with named columns,
 * <pre>
 * {
 *   "table": &lt;tableName>
 *   "data": [
 *     {
 *       "row": [&lt;component>, ...],
 *       "&lt;short col name>": &lt;value>,
 *       ...
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
 *       "cols": [
 *         {
 *           "col": [&lt;component>, ...],
 *           "val": &lt;value>
 *         },
 *         ...
 *       ]
 *     },
 *     ...
 *   ]
 * }
 * </pre>
 */
public class TableRowResult {
    private final String tableName;
    private final Iterable<RowResult<byte[]>> results;

    public TableRowResult(String tableName, Collection<RowResult<byte[]>> results) {
        this.tableName = Preconditions.checkNotNull(tableName);
        this.results = Preconditions.checkNotNull(results);
    }

    public String getTableName() {
        return tableName;
    }

    public Iterable<RowResult<byte[]>> getResults() {
        return results;
    }

    @Override
    public String toString() {
        return "TableRowResult [tableName=" + tableName + ", results=" + results + "]";
    }
}
