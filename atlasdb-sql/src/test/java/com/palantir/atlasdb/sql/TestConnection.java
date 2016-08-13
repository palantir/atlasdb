package com.palantir.atlasdb.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.palantir.atlasdb.sql.jdbc.AtlasJdbcDriver;

public class TestConnection {
    public static Connection create(String configFilename) throws ClassNotFoundException, SQLException {
        Class.forName(AtlasJdbcDriver.class.getName());
        final String configFilePath = ConnectionTest.class.getClassLoader().getResource(configFilename).getFile();
        final String uri = "jdbc:atlas?configFile=" + configFilePath;
        return DriverManager.getConnection(uri);
    }
}
