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

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.logsafe.Preconditions;

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
        this.tableName = Preconditions.checkNotNull(tableName, "tableName must not be null!");
        this.cells = Preconditions.checkNotNull(cells, "cells must not be null!");
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
