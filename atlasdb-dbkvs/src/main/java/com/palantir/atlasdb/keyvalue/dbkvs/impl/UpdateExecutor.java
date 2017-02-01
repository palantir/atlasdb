/**
 * Copyright 2017 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.nexus.db.sql.AgnosticResultSet;
import com.palantir.nexus.db.sql.PalantirSqlConnection;

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
                cell.getRowName(),
                cell.getColumnName(),
                ts,
                newValue,
                cell.getRowName(),
                cell.getColumnName(),
                ts,
                oldValue
        };

        String tableName = prefixedTableNames.get(tableRef);
        String sqlString = "/* UPDATE (" + tableName + ") */"
                + " UPDATE " + tableName + ""
                + " SET row_name = ?, col_name = ?, ts = ?, val = ?"
                + " WHERE row_name = ?"
                + " AND col_name = ?"
                + " AND ts = ?"
                + " AND val = ?";
        int updated = ((PalantirSqlConnection) conns.get()).updateCountRowsUnregisteredQuery(sqlString,
                args);
        if (updated == 0) {
            byte[] actualValue = getActualValue(tableName, cell, ts);
            throw new CheckAndSetException(cell, tableRef, oldValue, ImmutableList.of(actualValue));
        }
    }

    private byte[] getActualValue(String tableName, Cell cell, long ts) {
        Object[] args = new Object[] {
                cell.getRowName(),
                cell.getColumnName(),
                ts
        };

        String sqlString = "/* SELECT (" + tableName + ") */"
                + " SELECT val from " + tableName + ""
                + " WHERE row_name = ?"
                + " AND col_name = ?"
                + " AND ts = ?";
        AgnosticResultSet results = conns.get().selectResultSetUnregisteredQuery(sqlString, args);
        if (results.size() < 1) {
            return PtBytes.EMPTY_BYTE_ARRAY;
        } else {
            //noinspection deprecation
            return MoreObjects.firstNonNull(
                    Iterables.getOnlyElement(results.rows()).getBytes("val"),
                    PtBytes.EMPTY_BYTE_ARRAY);
        }
    }
}
