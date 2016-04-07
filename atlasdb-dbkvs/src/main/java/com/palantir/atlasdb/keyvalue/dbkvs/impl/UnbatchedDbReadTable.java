package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import java.util.Map;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.nexus.db.sql.AgnosticLightResultRow;
import com.palantir.nexus.db.sql.AgnosticLightResultSet;

public class UnbatchedDbReadTable extends AbstractDbReadTable {

    protected UnbatchedDbReadTable(ConnectionSupplier conns,
                                   DbQueryFactory queryFactory) {
        super(conns, queryFactory);
    }

    @Override
    public ClosableIterator<AgnosticLightResultRow> getLatestRowsInternal(Iterable<byte[]> rows,
                                                                          ColumnSelection columns,
                                                                          long ts,
                                                                          boolean includeValues) {
        return run(queryFactory.getLatestRowsQuery(rows, ts, columns, includeValues));
    }

    @Override
    public ClosableIterator<AgnosticLightResultRow> getLatestRowsInternal(Map<byte[], Long> rows,
                                                                          ColumnSelection columns,
                                                                          boolean includeValues) {
        return run(queryFactory.getLatestRowsQuery(rows.entrySet(), columns, includeValues));
    }

    @Override
    public ClosableIterator<AgnosticLightResultRow> getAllRowsInternal(Iterable<byte[]> rows,
                                                                       ColumnSelection columns,
                                                                       long ts,
                                                                       boolean includeValues) {
        return run(queryFactory.getAllRowsQuery(rows, ts, columns, includeValues));
    }

    @Override
    public ClosableIterator<AgnosticLightResultRow> getAllRowsInternal(Map<byte[], Long> rows,
                                                                       ColumnSelection columns,
                                                                       boolean includeValues) {
        return run(queryFactory.getAllRowsQuery(rows.entrySet(), columns, includeValues));
    }

    @Override
    public ClosableIterator<AgnosticLightResultRow> getLatestCellsInternal(Iterable<Cell> cells,
                                                                           long ts,
                                                                           boolean includeValue) {
        return run(queryFactory.getLatestCellsQuery(cells, ts, includeValue));
    }

    @Override
    public ClosableIterator<AgnosticLightResultRow> getLatestCellsInternal(Map<Cell, Long> cells,
                                                                           boolean includeValue) {
        return run(queryFactory.getLatestCellsQuery(cells.entrySet(), includeValue));
    }

    @Override
    public ClosableIterator<AgnosticLightResultRow> getAllCellsInternal(Iterable<Cell> cells,
                                                                        long ts,
                                                                        boolean includeValue) {
        return run(queryFactory.getAllCellsQuery(cells, ts, includeValue));
    }

    @Override
    public ClosableIterator<AgnosticLightResultRow> getAllCellsInternal(Map<Cell, Long> cells,
                                                                        boolean includeValue) {
        return run(queryFactory.getAllCellsQuery(cells.entrySet(), includeValue));
    }

    @Override
    protected ClosableIterator<AgnosticLightResultRow> run(FullQuery query) {
        AgnosticLightResultSet results = conns.get().selectLightResultSetUnregisteredQuery(
                query.getQuery(), query.getArgs());
        return ClosableIterators.wrap(results.iterator(), results);
    }
}
