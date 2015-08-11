package com.palantir.atlas.api;

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
        this.tableName = tableName;
        this.startRow = startRow;
        this.endRow = endRow;
        this.columns = columns;
        this.batchSize = batchSize;
    }

    public TableRange withStartRow(byte[] startRow) {
        return new TableRange(tableName, startRow, endRow, columns, batchSize);
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
