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
package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import com.google.common.collect.Iterables;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.nexus.db.sql.AgnosticLightResultSet;
import com.palantir.nexus.db.sql.PalantirSqlConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class UpdateExecutor {
    private final ConnectionSupplier conns;
    private final TableReference tableRef;
    private final PrefixedTableNames prefixedTableNames;

    public UpdateExecutor(ConnectionSupplier conns, TableReference tableRef, PrefixedTableNames prefixedTableNames) {
        this.conns = conns;
        this.tableRef = tableRef;
        this.prefixedTableNames = prefixedTableNames;
    }

    public void update(Cell cell, long ts, byte[] oldValue, byte[] newValue) {
        Object[] args = new Object[] {
            cell.getRowName(), cell.getColumnName(), ts, newValue, cell.getRowName(), cell.getColumnName(), ts, oldValue
        };

        String prefixedTableName = prefixedTableNames.get(tableRef, conns);
        String sqlString = "/* UPDATE (" + prefixedTableName + ") */"
                + " UPDATE " + prefixedTableName + ""
                + " SET row_name = ?, col_name = ?, ts = ?, val = ?"
                + " WHERE row_name = ?"
                + " AND col_name = ?"
                + " AND ts = ?"
                + " AND val = ?";

        PalantirSqlConnection connection = (PalantirSqlConnection) conns.get();
        while (true) {
            int updated = connection.updateCountRowsUnregisteredQuery(sqlString, args);
            if (updated != 0) {
                return;
            }
            List<byte[]> currentValue = getCurrentValue(connection, cell, ts, prefixedTableName);
            byte[] onlyValue = Iterables.getOnlyElement(currentValue, null);
            if (!Arrays.equals(onlyValue, oldValue)) {
                throw new CheckAndSetException(cell, tableRef, oldValue, currentValue);
            }
        }
    }

    // A list, but in practice at most one value
    private List<byte[]> getCurrentValue(
            PalantirSqlConnection connection, Cell cell, long ts, String prefixedTableName) {
        String sqlString = "/* SELECT (" + prefixedTableName + ") */"
                + " SELECT val FROM " + prefixedTableName
                + " WHERE row_name = ?"
                + " AND col_name = ?"
                + " AND ts = ?";
        try (AgnosticLightResultSet results = connection.selectLightResultSetUnregisteredQuery(
                sqlString, cell.getRowName(), cell.getColumnName(), ts)) {
            List<byte[]> actualValues = new ArrayList<>();
            results.forEach(row -> actualValues.add(row.getBytes(DbKvs.VAL)));
            return actualValues;
        }
    }
}
