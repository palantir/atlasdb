/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.nexus.db;

import com.palantir.exception.PalantirSqlException;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.sql.Connections;
import java.sql.Connection;
import javax.annotation.Nullable;

/**
 * Extensibility point for adding more type of databases.
 *
 * @author akashj
 */
public enum DBType {
    // AJ: the oracle jdbc url is missing a final "paren" - processing in DBMgr will fix this...
    ORACLE("oracle.jdbc.driver.OracleDriver", "SELECT 1 FROM dual", true),
    POSTGRESQL("org.postgresql.Driver", "SELECT 1", true),
    H2_MEMORY("org.h2.Driver", "SELECT 1", true);

    private final String driver;
    private final String testQuery;
    private final boolean hasGIS;

    public boolean supportsGIS() {
        return hasGIS;
    }

    DBType(String driver, String testQuery, boolean hasGIS) {
        this.driver = driver;
        this.testQuery = testQuery;
        this.hasGIS = hasGIS;
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

    public static DBType getTypeFromConnection(Connection c) throws PalantirSqlException {
        String url = Connections.getUrl(c);
        if (url.startsWith("jdbc:oracle:thin")) {
            return ORACLE;
        }
        if (url.startsWith("jdbc:pgsql")) {
            return POSTGRESQL;
        }
        if (url.startsWith("jdbc:postgresql")) {
            return POSTGRESQL;
        }
        if (url.startsWith("jdbc:h2:mem:")) {
            return H2_MEMORY;
        }
        throw new SafeRuntimeException("Unable to parse JDBC URL");
    }
}
