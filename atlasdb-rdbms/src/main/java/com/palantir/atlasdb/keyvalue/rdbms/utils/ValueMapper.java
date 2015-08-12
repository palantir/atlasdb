package com.palantir.atlasdb.keyvalue.rdbms.utils;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import com.palantir.atlasdb.keyvalue.api.Value;

public class ValueMapper implements ResultSetMapper<Value> {
    @Override
    public Value map(int index, ResultSet r, StatementContext ctx) throws SQLException {
        byte[] content = r.getBytes(Columns.CONTENT.toString());
        long timestamp = TimestampMapper.instance().map(index, r, ctx);
        return Value.create(content, timestamp);
    }

    private ValueMapper() {}
    private static ValueMapper instance;
    public static ValueMapper instance() {
        if (instance == null) {
            ValueMapper ret = new ValueMapper();
            instance = ret;
        }
        return instance;
    }
}
