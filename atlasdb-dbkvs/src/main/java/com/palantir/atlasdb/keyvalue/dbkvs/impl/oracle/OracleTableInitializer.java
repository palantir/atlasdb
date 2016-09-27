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

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbTableInitializer;
import com.palantir.exception.PalantirSqlException;

public class OracleTableInitializer implements DbTableInitializer {
    private static final Logger log = LoggerFactory.getLogger(OracleTableInitializer.class);

    private final ConnectionSupplier connectionSupplier;

    public OracleTableInitializer(ConnectionSupplier conns) {
        this.connectionSupplier = conns;
    }

    @Override
    public void createUtilityTables(String tablePrefix) {
        executeIgnoringError(
                "CREATE TYPE " + tablePrefix + "CELL_TS AS OBJECT ("
                        + "row_name   RAW(2000),"
                        + "col_name   RAW(2000),"
                        + "max_ts     NUMBER(20)"
                        + ")",
                "name is already used by an existing object");

        executeIgnoringError(
                "CREATE TYPE " + tablePrefix + "CELL_TS_TABLE AS TABLE OF " + tablePrefix + "CELL_TS",
                "name is already used by an existing object"
        );

        executeIgnoringError(
                "CREATE SEQUENCE " + tablePrefix + "OVERFLOW_SEQ ",
                "name is already used by an existing object");
    }

    @Override
    public void createMetadataTable(TableReference metadataTable, String tablePrefix) {
        final String metadataTableName = tablePrefix + metadataTable.getQualifiedName();
        executeIgnoringError(
                String.format(
                        "CREATE TABLE %s ("
                                + "table_name varchar(2000) NOT NULL,"
                                + "table_size NUMBER(38) NOT NULL,"
                                + "value      LONG RAW NULL,"
                                + "CONSTRAINT pk_%s PRIMARY KEY (table_name)"
                                + ")",
                        metadataTableName, metadataTableName),
                "name is already used by an existing object");

        executeIgnoringError(
                String.format(
                        "CREATE UNIQUE INDEX unique_%s_index ON %s (lower(table_name))",
                        metadataTableName, metadataTableName),
                "name is already used by an existing object");
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
