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
package com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbTableInitializer;
import com.palantir.exception.PalantirSqlException;

public class OracleTableInitializer implements DbTableInitializer {
    private static final Logger log = LoggerFactory.getLogger(OracleTableInitializer.class);

    private static final String ORACLE_ALREADY_EXISTS_ERROR = "name is already used by an existing object";
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
                ORACLE_ALREADY_EXISTS_ERROR);

        executeIgnoringError(
                "CREATE TYPE " + config.tablePrefix() + "CELL_TS_TABLE AS TABLE OF " + config.tablePrefix() + "CELL_TS",
                ORACLE_ALREADY_EXISTS_ERROR
        );

        executeIgnoringError(
                "CREATE SEQUENCE " + config.tablePrefix() + AtlasDbConstants.ORACLE_OVERFLOW_SEQUENCE + " INCREMENT BY "
                        + OverflowSequenceSupplier.OVERFLOW_ID_CACHE_SIZE,
                ORACLE_ALREADY_EXISTS_ERROR);

        executeIgnoringError(
                String.format(
                        "CREATE TABLE %s ("
                                + "table_name varchar(2000) NOT NULL,"
                                + "short_table_name varchar(30) NOT NULL,"
                                + "CONSTRAINT pk_%s PRIMARY KEY (short_table_name),"
                                + "CONSTRAINT un_%s UNIQUE (table_name)"
                                + ")",
                        AtlasDbConstants.ORACLE_NAME_MAPPING_TABLE, AtlasDbConstants.ORACLE_NAME_MAPPING_TABLE, AtlasDbConstants.ORACLE_NAME_MAPPING_TABLE),
                ORACLE_ALREADY_EXISTS_ERROR);
    }

    @Override
    public void createMetadataTable(String metadataTableName) {
        executeIgnoringError(
                String.format(
                        "CREATE TABLE %s ("
                                + "table_name varchar(2000) NOT NULL,"
                                + "table_size NUMBER(38) NOT NULL,"
                                + "value      LONG RAW NULL,"
                                + "CONSTRAINT pk_%s PRIMARY KEY (table_name)"
                                + ")",
                        metadataTableName, metadataTableName),
                ORACLE_ALREADY_EXISTS_ERROR);

        executeIgnoringError(
                String.format(
                        "CREATE UNIQUE INDEX unique_%s_index ON %s (lower(table_name))",
                        metadataTableName, metadataTableName),
                ORACLE_ALREADY_EXISTS_ERROR);
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
