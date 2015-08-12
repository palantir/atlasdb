package com.palantir.atlasdb.keyvalue.rdbms.utils;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import com.palantir.atlasdb.keyvalue.api.Cell;


public class CellMapper implements ResultSetMapper<Cell> {
    @Override
    public Cell map(int index, ResultSet r, StatementContext ctx) throws SQLException {
        byte[] row = r.getBytes(Columns.ROW.toString());
        byte[] column = r.getBytes(Columns.COLUMN.toString());
        return Cell.create(row, column);
    }

    private CellMapper() {}
    private static CellMapper instance;
    public static CellMapper instance() {
        if (instance == null) {
            CellMapper ret = new CellMapper();
            instance = ret;
        }
        return instance;
    }
}
