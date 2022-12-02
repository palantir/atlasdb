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
package com.palantir.atlasdb.keyvalue.dbkvs.timestamp;

import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.nexus.db.DBType;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Locale;

public final class ConnectionDbTypes {
    private static final SafeLogger log = SafeLoggerFactory.get(ConnectionDbTypes.class);

    private ConnectionDbTypes() {
        // Utility class
    }

    public static DBType getDbType(Connection conn) {
        try {
            DatabaseMetaData metaData = conn.getMetaData();
            String driverName = metaData.getDriverName().toLowerCase(Locale.ROOT);
            if (driverName.contains("oracle")) {
                return DBType.ORACLE;
            } else if (driverName.contains("postgres")) {
                return DBType.POSTGRESQL;
            } else if (driverName.contains("h2")) {
                return DBType.H2_MEMORY;
            } else {
                log.error(
                        "Could not determine database type from connection with driver name {}",
                        SafeArg.of("driverName", driverName));
                return null;
            }
        } catch (SQLException e) {
            log.error("Could not determine database type from connection.", e);
            return null;
        }
    }
}
