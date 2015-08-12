package com.palantir.atlasdb.keyvalue.rdbms.utils;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

public class TimestampMapper implements ResultSetMapper<Long> {
    @Override
    public Long map(int index, ResultSet r, StatementContext ctx) throws SQLException {
        return r.getLong(Columns.TIMESTAMP.toString());
    }

    private TimestampMapper() {}
    private static TimestampMapper instance;
    public static TimestampMapper instance() {
        if (instance == null) {
            TimestampMapper ret = new TimestampMapper();
            instance = ret;
        }
        return instance;
    }
}
