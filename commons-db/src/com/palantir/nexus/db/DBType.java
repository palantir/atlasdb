package com.palantir.nexus.db;

import java.sql.Connection;

import javax.annotation.Nullable;

import com.palantir.exception.PalantirSqlException;
import com.palantir.sql.Connections;

/**
 * Extensibility point for adding more type of databases.
 *
 * @author akashj
 */
public enum DBType {
    // AJ: the oracle jdbc url is missing a final "paren" - processing in DBMgr will fix this...
    ORACLE("jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL={PROTOCOL})(HOST={HOST})(PORT={PORT}))(CONNECT_DATA=(SID={SID}))", "oracle.jdbc.driver.OracleDriver", "SELECT 1 FROM dual", true),
    POSTGRESQL("jdbc:postgresql://{HOST}:{PORT}/{DBNAME}", "org.postgresql.Driver", "SELECT 1", true),
    H2_MEMORY("jdbc:h2:mem:", "org.h2.Driver", "SELECT 1", true);

    private final String defaultUrl;
    private final String driver;
    private final String testQuery;
    private final boolean hasGIS;

    public boolean supportsGIS() {
        return hasGIS;
    }

    private DBType(String defaultUrl, String driver, String testQuery, boolean hasGIS) {
        this.defaultUrl = defaultUrl;
        this.driver = driver;
        this.testQuery = testQuery;
        this.hasGIS = hasGIS;
    }

    public String getDefaultUrl() {
        return defaultUrl;
    }

    public String getDriverName() {
        return driver;
    }

    public String getTestQuery() {
        return testQuery;
    }

    /**
     * Looks up a DBType by name.  Names are not case-sensitive.
     *
     * @param strName the name of the type to lookup, typically corresponds
     *        to the type name, e.g. DBType.ORACLE.toString();
     * @return the property DBType, or null if none exists
     */
    @Nullable
    public static DBType getTypeByName(@Nullable String strName) {
        if (strName == null) {
            return null;
        }
        return DBType.valueOf(strName.toUpperCase());
    }

    public static DBType getTypeFromConnection(Connection c)
            throws PalantirSqlException {
        String url = Connections.getUrl(c);
        if (url.startsWith("jdbc:oracle:thin"))
            return ORACLE;
        if (url.startsWith("jdbc:pgsql"))
            return POSTGRESQL;
        if (url.startsWith("jdbc:postgresql"))
            return POSTGRESQL;
        if (url.startsWith("jdbc:h2:mem:"))
            return H2_MEMORY;
        throw new RuntimeException("Unable to parse JDBC URL");
    }
}
