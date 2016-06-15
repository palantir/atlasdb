/**
 * Copyright 2015 Palantir Technologies
 * <p>
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://opensource.org/licenses/BSD-3-Clause
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.dbkvs.impl.postgres;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbTableInitializer;
import com.palantir.exception.PalantirSqlException;

public class PostgresTableInitializer implements DbTableInitializer {
    private static final Logger log = LoggerFactory.getLogger(PostgresTableInitializer.class);

    private final ConnectionSupplier connectionSupplier;

    public PostgresTableInitializer(ConnectionSupplier conns) {
        this.connectionSupplier = conns;
    }

    @Override
    public void createUtilityTables() {
        executeIgnoringError(
                "CREATE TABLE dual (id BIGINT)",
                "already exists"
        );

        connectionSupplier.get().executeUnregisteredQuery(
                "INSERT INTO dual (id) SELECT 1 WHERE NOT EXISTS ( SELECT id FROM dual WHERE id = 1 )"
        );
    }

    @Override
    public void createMetadataTable(String metadataTableName) {
        executeIgnoringError(
                "CREATE TABLE " + metadataTableName + " (" +
                        "  table_name VARCHAR(2000) NOT NULL," +
                        "  table_size BIGINT NOT NULL," +
                        "  value      BYTEA NULL," +
                        "  gc_ts      INT8 DEFAULT -1," +
                        "  CONSTRAINT pk_" + metadataTableName + " PRIMARY KEY (table_name) " +
                        ")",
                "already exists");
    }

    private void executeIgnoringError(String sql, String errorToIgnore) {
        try {
            connectionSupplier.get().executeUnregisteredQuery(sql);
        } catch (PalantirSqlException e) {
            if (!e.getMessage().contains(errorToIgnore)) {
                log.error(e.getMessage(), e);
                throw e;
            }
        }
    }
}
