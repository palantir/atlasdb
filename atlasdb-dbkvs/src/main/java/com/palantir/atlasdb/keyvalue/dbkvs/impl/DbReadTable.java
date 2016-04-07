package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import java.util.Collection;
import java.util.Map;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.common.base.ClosableIterator;
import com.palantir.nexus.db.sql.AgnosticLightResultRow;

public interface DbReadTable {
    ClosableIterator<AgnosticLightResultRow> getLatestRows(Iterable<byte[]> rows, ColumnSelection columns, long ts, boolean includeValue);
    ClosableIterator<AgnosticLightResultRow> getLatestRows(Map<byte[], Long> rows, ColumnSelection columns, boolean includeValue);
    ClosableIterator<AgnosticLightResultRow> getAllRows(Iterable<byte[]> rows, ColumnSelection columns, long ts, boolean includeValue);
    ClosableIterator<AgnosticLightResultRow> getAllRows(Map<byte[], Long> rows, ColumnSelection columns, boolean includeValue);
    ClosableIterator<AgnosticLightResultRow> getLatestCells(Iterable<Cell> cells, long ts, boolean includeValue);
    ClosableIterator<AgnosticLightResultRow> getLatestCells(Map<Cell, Long> cells, boolean includeValue);
    ClosableIterator<AgnosticLightResultRow> getAllCells(Iterable<Cell> cells, long ts, boolean includeValue);
    ClosableIterator<AgnosticLightResultRow> getAllCells(Map<Cell, Long> cells, boolean includeValue);
    ClosableIterator<AgnosticLightResultRow> getRange(RangeRequest range, long ts, int maxRows);
    boolean hasOverflowValues();
    ClosableIterator<AgnosticLightResultRow> getOverflow(Collection<OverflowValue> overflowIds);
}
