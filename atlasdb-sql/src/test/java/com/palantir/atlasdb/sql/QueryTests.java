package com.palantir.atlasdb.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Callable;

import com.palantir.atlasdb.sql.jdbc.AtlasJdbcDriver;

public abstract class QueryTests {

    public static final String IN_MEMORY_TEST_CONFIG = "memoryTestConfig.yml";

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

    public static void assertFails(Callable<?> c) {
        try {
            c.call();
            throw new RuntimeException("the call did not fail");
        } catch (Exception e) {
            // success
        }
    }

    public static Connection connect(String configFilename) throws ClassNotFoundException, SQLException {
        Class.forName(AtlasJdbcDriver.class.getName());
        final String configFilePath = ConnectionTest.class.getClassLoader().getResource(configFilename).getFile();
        final String uri = "jdbc:atlas?configFile=" + configFilePath;
        return DriverManager.getConnection(uri);
    }
}
