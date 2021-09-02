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
package com.palantir.atlasdb.keyvalue.dbkvs.impl.postgres;

import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbTableInitializer;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.PrimaryKeyConstraintNames;
import com.palantir.exception.PalantirSqlException;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;

public class PostgresTableInitializer implements DbTableInitializer {
    private static final SafeLogger log = SafeLoggerFactory.get(PostgresTableInitializer.class);

    private final ConnectionSupplier connectionSupplier;

    public PostgresTableInitializer(ConnectionSupplier conns) {
        this.connectionSupplier = conns;
    }

    @Override
    public void createUtilityTables() {
        executeIgnoringError(
                "CREATE TABLE dual (id BIGINT, CONSTRAINT "
                        + PrimaryKeyConstraintNames.get("dual")
                        + " PRIMARY KEY (id))",
                "already exists");

        executeIgnoringError(
                "INSERT INTO dual (id) SELECT 1 WHERE NOT EXISTS ( SELECT id FROM dual WHERE id = 1 )",
                "duplicate key");
    }

    @Override
    public void createMetadataTable(String metadataTableName) {
        executeIgnoringError(
                String.format(
                        "CREATE TABLE %s ("
                                + "  table_name VARCHAR(2000) NOT NULL,"
                                + "  table_size BIGINT NOT NULL,"
                                + "  value      BYTEA NULL,"
                                + "  CONSTRAINT %s PRIMARY KEY (table_name) "
                                + ")",
                        metadataTableName, PrimaryKeyConstraintNames.get(metadataTableName)),
                "already exists");

        executeIgnoringError(
                String.format(
                        "CREATE UNIQUE INDEX unique_lower_case_%s_index ON %s (lower(table_name))",
                        metadataTableName, metadataTableName),
                "already exists");
    }

    private void executeIgnoringError(String sql, String errorToIgnore) {
        try {
            connectionSupplier.get().executeUnregisteredQuery(sql);
        } catch (PalantirSqlException e) {
            if (!e.getMessage().contains(errorToIgnore)) {
                log.error("Error occurred trying to execute the Postgres query {}", UnsafeArg.of("sql", sql), e);
                throw e;
            }
        }
    }
}
