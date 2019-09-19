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

import com.palantir.logsafe.Preconditions;
import java.util.Arrays;

/**
 * <pre>
 * {
 *   "table": &lt;tableName>,
 *   "batch_size": &lt;batchSize> (default 2000),
 *   "cols": [&lt;short col name>, ...] (default all columns),
 *   "raw_start": &lt;raw start row>,
 *   "raw_end": &lt;raw end row>,
 *   "prefix": [&lt;component>, ...],
 *   "start": [&lt;component>, ...],
 *   "end": [&lt;component>, ...]
 * }
 * </pre>
 * If prefix is specified, the given (possibly partial) row is used as the prefix of the range.
 * Otherwise, raw_start, start, raw_end, and end are used to determine the start and end of the range.
 * <p>
 * raw_start and raw_end take precedence over start and end if both happen to be specified.
 * raw_start and raw_end are base64 encoded strings of a complete row. They are not intended to be
 * used directly by clients; they will be used in the table range returned as part of a range token.
 * <p>
 * If a start row is not specified through prefix, raw_start, or start, the start of the range is
 * unbounded. Similarly, if the end row is not specified the end of the range is unbounded.
 */
public class TableRange {
    private final String tableName;
    private final byte[] startRow;
    private final byte[] endRow;
    private final Iterable<byte[]> columns;
    private final int batchSize;

    public TableRange(String tableName,
                      byte[] startRow,
                      byte[] endRow,
                      Iterable<byte[]> columns,
                      int batchSize) {
        this.tableName = Preconditions.checkNotNull(tableName, "tableName must not be null!");
        this.startRow = startRow;
        this.endRow = endRow;
        this.columns = columns;
        this.batchSize = batchSize;
    }

    public TableRange withStartRow(byte[] startingRow) {
        return new TableRange(tableName, startingRow, endRow, columns, batchSize);
    }

    public String getTableName() {
        return tableName;
    }

    public byte[] getStartRow() {
        return startRow;
    }

    public byte[] getEndRow() {
        return endRow;
    }

    public Iterable<byte[]> getColumns() {
        return columns;
    }

    public int getBatchSize() {
        return batchSize;
    }

    @Override
    public String toString() {
        return "TableRange [tableName=" + tableName + ", startRow=" + Arrays.toString(startRow)
                + ", endRow=" + Arrays.toString(endRow) + ", columns=" + columns + ", batchSize="
                + batchSize + "]";
    }
}
