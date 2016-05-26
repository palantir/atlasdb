package com.palantir.atlasdb.keyvalue.dbkvs.timestamp;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.nexus.db.DBType;

public class ConnectionDbTypes {

    private static final Logger log = LoggerFactory.getLogger(ConnectionDbTypes.class);

    public static DBType getDBType(Connection conn) {
        try {
            DatabaseMetaData metaData = conn.getMetaData();
            String driverName = metaData.getDriverName().toLowerCase();
            if (driverName.contains("oracle")) {
                return DBType.ORACLE;
            } else if (driverName.contains("postgres")) {
                return DBType.POSTGRESQL;
            } else if (driverName.contains("h2")) {
                return DBType.H2_MEMORY;
            } else {
                log.error("Could not determine database type from connection with driver name " + driverName);
                return null;
            }
        } catch (SQLException e) {
            log.error("Could not determine database type from connection.", e);
            return null;
        }
    }
}
