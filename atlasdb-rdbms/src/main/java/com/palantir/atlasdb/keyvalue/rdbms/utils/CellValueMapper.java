package com.palantir.atlasdb.keyvalue.rdbms.utils;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.util.Pair;

public class CellValueMapper implements ResultSetMapper<Pair<Cell, Value>> {
    private static CellValueMapper instance;

    @Override
    public Pair<Cell, Value> map(int index, ResultSet r, StatementContext ctx) throws SQLException {
        Cell cell = CellMapper.instance().map(index, r, ctx);
        Value value = ValueMapper.instance().map(index, r, ctx);
        return Pair.create(cell, value);
    }

    public static CellValueMapper instance() {
        if (instance == null) {
            CellValueMapper ret = new CellValueMapper();
            instance = ret;
        }
        return instance;
    }

    private CellValueMapper() {}
}
