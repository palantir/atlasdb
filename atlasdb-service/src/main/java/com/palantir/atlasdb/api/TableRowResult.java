/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
