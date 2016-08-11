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
    private Logger log = LoggerFactory.getLogger(ConnectionTest.class);

    @Test
    public void testInMemoryConnection() throws ClassNotFoundException, SQLException {
        connect("memoryTestConfig.yml");
    }

    // TODO: start postgres in a docker container
    @Test @Ignore
    public void testPostgresConnection() throws ClassNotFoundException, SQLException {
        connect("postgresTestConfig.yml");
    }

    private void connect(String name) throws ClassNotFoundException, SQLException {
        Connection connection = null;
        try {
            Class.forName(AtlasJdbcDriver.class.getName());
            final java.net.URL resource = ConnectionTest.class.getClassLoader().getResource(name);
            assert resource != null;
            final String URL = "jdbc:atlas?configFile=" + resource.getFile();
            log.debug("url {}", URL);
            connection = DriverManager.getConnection(URL);
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }
}
