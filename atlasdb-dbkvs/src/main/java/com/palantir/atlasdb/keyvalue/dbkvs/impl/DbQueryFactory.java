package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import java.util.Collection;
import java.util.Map;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;

public interface DbQueryFactory {
    FullQuery getLatestRowQuery(byte[] row, long ts, ColumnSelection columns, boolean includeValue);
    FullQuery getLatestRowsQuery(Iterable<byte[]> rows, long ts, ColumnSelection columns, boolean includeValue);
    FullQuery getLatestRowsQuery(Collection<Map.Entry<byte[], Long>> rows, ColumnSelection columns, boolean includeValue);

    FullQuery getAllRowQuery(byte[] row, long ts, ColumnSelection columns, boolean includeValue);
    FullQuery getAllRowsQuery(Iterable<byte[]> rows, long ts, ColumnSelection columns, boolean includeValue);
    FullQuery getAllRowsQuery(Collection<Map.Entry<byte[], Long>> rows, ColumnSelection columns, boolean includeValue);

    FullQuery getLatestCellQuery(Cell cell, long ts, boolean includeValue);
    FullQuery getLatestCellsQuery(Iterable<Cell> cells, long ts, boolean includeValue);
    FullQuery getLatestCellsQuery(Collection<Map.Entry<Cell, Long>> cells, boolean includeValue);

    FullQuery getAllCellQuery(Cell cell, long ts, boolean includeValue);
    FullQuery getAllCellsQuery(Iterable<Cell> cells, long ts, boolean includeValue);
    FullQuery getAllCellsQuery(Collection<Map.Entry<Cell, Long>> cells, boolean includeValue);

    FullQuery getRangeQuery(RangeRequest range, long ts, int maxRows);
    boolean hasOverflowValues();
    Collection<FullQuery> getOverflowQueries(Collection<OverflowValue> overflowIds);
}
