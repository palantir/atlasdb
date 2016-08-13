package com.palantir.atlasdb.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.sql.jdbc.AtlasJdbcDriver;

public class ConnectionTest {
    @Test
    public void testInMemoryConnection() throws ClassNotFoundException, SQLException {
        connect(QueryTests.IN_MEMORY_TEST_CONFIG);
    }

    private void connect(String name) throws ClassNotFoundException, SQLException {
        try (Connection c = QueryTests.connect(name)) {
            // success
        }
    }
}
