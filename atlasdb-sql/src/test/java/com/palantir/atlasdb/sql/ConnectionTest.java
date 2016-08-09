package com.palantir.atlasdb.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionTest {
    private Logger log = LoggerFactory.getLogger(ConnectionTest.class);

    @Test
    public void testConnection() throws ClassNotFoundException, SQLException {
        Connection connection = null;
        try {
            Class.forName(AtlasJdbcDriver.class.getName());
            final java.net.URL resource = ConnectionTest.class.getClassLoader().getResource("memoryTestConfig.yml");
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
