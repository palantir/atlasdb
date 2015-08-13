package com.palantir.atlasdb.rdbms.impl.util;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.palantir.atlasdb.rdbms.api.service.AtlasRdbmsResultSetHandler;

public final class ResultSetHandlers {

    public static final AtlasRdbmsResultSetHandler<Boolean> ROWS_EXIST = new AtlasRdbmsResultSetHandler<Boolean>() {
        @Override public Boolean handle(ResultSet resultSet) throws SQLException {
            return resultSet.next();
        }
    };

    private ResultSetHandlers() { /* uninstantiable */ }
}
