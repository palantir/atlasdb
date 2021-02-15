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
package com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleErrorConstants;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbTableInitializer;
import com.palantir.exception.PalantirSqlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OracleTableInitializer implements DbTableInitializer {
    private static final Logger log = LoggerFactory.getLogger(OracleTableInitializer.class);

    private final ConnectionSupplier connectionSupplier;
    private final OracleDdlConfig config;

    public OracleTableInitializer(ConnectionSupplier conns, OracleDdlConfig config) {
        this.connectionSupplier = conns;
        this.config = config;
    }

    @Override
    public void createUtilityTables() {
        executeIgnoringError(
                "CREATE TYPE " + config.tablePrefix() + "CELL_TS AS OBJECT ("
                        + "row_name   RAW(2000),"
                        + "col_name   RAW(2000),"
                        + "max_ts     NUMBER(20)"
                        + ")",
                OracleErrorConstants.ORACLE_ALREADY_EXISTS_ERROR);

        executeIgnoringError(
                "CREATE TYPE " + config.tablePrefix() + "CELL_TS_TABLE AS TABLE OF " + config.tablePrefix() + "CELL_TS",
                OracleErrorConstants.ORACLE_ALREADY_EXISTS_ERROR);

        executeIgnoringError(
                String.format(
                        "CREATE TABLE %s ("
                                + "table_name varchar(2000) NOT NULL,"
                                + "short_table_name varchar(%d) NOT NULL,"
                                + "CONSTRAINT %s PRIMARY KEY (table_name),"
                                + "CONSTRAINT unique_%s UNIQUE (short_table_name)"
                                + ")",
                        AtlasDbConstants.ORACLE_NAME_MAPPING_TABLE,
                        AtlasDbConstants.ATLASDB_ORACLE_TABLE_NAME_LIMIT,
                        AtlasDbConstants.ORACLE_NAME_MAPPING_PK_CONSTRAINT,
                        AtlasDbConstants.ORACLE_NAME_MAPPING_TABLE),
                OracleErrorConstants.ORACLE_ALREADY_EXISTS_ERROR);

        executeIgnoringError(
                "CREATE SEQUENCE " + config.tablePrefix() + AtlasDbConstants.ORACLE_OVERFLOW_SEQUENCE + " INCREMENT BY "
                        + OverflowSequenceSupplier.OVERFLOW_ID_CACHE_SIZE,
                OracleErrorConstants.ORACLE_ALREADY_EXISTS_ERROR);
    }

    @Override
    public void createMetadataTable(String metadataTableName) {
        executeIgnoringError(
                String.format(
                        "CREATE TABLE %s ("
                                + "table_name varchar(2000) NOT NULL,"
                                + "table_size NUMBER(38) NOT NULL,"
                                + "value      LONG RAW NULL,"
                                + "CONSTRAINT %s PRIMARY KEY (table_name)"
                                + ")",
                        metadataTableName, PrimaryKeyConstraintNames.get(metadataTableName)),
                OracleErrorConstants.ORACLE_ALREADY_EXISTS_ERROR);

        executeIgnoringError(
                String.format(
                        "CREATE UNIQUE INDEX unique_%s_index ON %s (lower(table_name))",
                        metadataTableName, metadataTableName),
                OracleErrorConstants.ORACLE_ALREADY_EXISTS_ERROR);
    }

    private void executeIgnoringError(String sql, String errorToIgnore) {
        try {
            connectionSupplier.get().executeUnregisteredQuery(sql);
        } catch (PalantirSqlException e) {
            if (!e.getMessage().contains(errorToIgnore)) {
                log.error("Error occurred trying to execute the query {}", sql, e);
                throw e;
            }
        }
    }
}
