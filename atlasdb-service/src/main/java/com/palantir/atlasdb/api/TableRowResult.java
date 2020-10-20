/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.api;

import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.logsafe.Preconditions;
import java.util.Collection;

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
        this.tableName = Preconditions.checkNotNull(tableName, "tableName must not be null!");
        this.results = Preconditions.checkNotNull(results, "results must not be null!");
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
