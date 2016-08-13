package com.palantir.atlasdb.sql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Callable;

public abstract class QueryTests {
    private QueryTests() {
        // uninstantiable
    }

    public static int count(ResultSet results) throws SQLException {
        int i = 0;
        while (results.next()) {
            i++;
        }
        return i;
    }

    public static boolean fails(Callable<?> c) {
        try {
            c.call();
            return false;
        } catch (Exception e) {
            return true;
        }
    }
}
