/**
 * Copyright 2015 Palantir Technologies
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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.dbkvs.DdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleDdlConfig;
import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.sql.AgnosticResultSet;
import com.palantir.nexus.db.sql.ExceptionCheck;

public class SimpleDbWriteTable implements DbWriteTable {
    protected final TableReference tableRef;
    protected final ConnectionSupplier conns;
    protected final DdlConfig config;

    public SimpleDbWriteTable(TableReference tableRef,
                              ConnectionSupplier conns,
                              DdlConfig config) {
        this.tableRef = tableRef;
        this.conns = conns;
        this.config = config;
    }

    @Override
    public void put(Collection<Map.Entry<Cell, byte[]>> data, long ts) {
        List<Object[]> args = Lists.newArrayListWithCapacity(data.size());
        for (Entry<Cell, byte[]> entry : data) {
            Cell cell = entry.getKey();
            byte[] val = entry.getValue();
            args.add(new Object[] { cell.getRowName(), cell.getColumnName(), ts, val });
        }
        put(args);
    }

    @Override
    public void put(Collection<Map.Entry<Cell, Value>> data) {
        List<Object[]> args = Lists.newArrayListWithCapacity(data.size());
        for (Entry<Cell, Value> entry : data) {
            Cell cell = entry.getKey();
            Value val = entry.getValue();
            args.add(new Object[] { cell.getRowName(), cell.getColumnName(), val.getTimestamp(), val.getContents() });
        }
        put(args);
    }

    private void put(List<Object[]> args) {
        try {
            //        if (config.type().equals(OracleDdlConfig.TYPE)) {
            //            return ((OracleDdlConfig) config)
            //                    .tableNameMapper()
            //                    .getShortPrefixedTableName(config.tablePrefix(), tableRef);
            //        }
            //        return config.tablePrefix() + tableRef;
            conns.get().insertManyUnregisteredQuery("/* INSERT_ONE (" + tableRef + ") */"
                    + " INSERT INTO " + getInternalTableName() + " (row_name, col_name, ts, val) "
                    + " VALUES (?, ?, ?, ?) ",
                    args);
        } catch (PalantirSqlException e) {
            if (ExceptionCheck.isUniqueConstraintViolation(e)) {
                throw new KeyAlreadyExistsException("primary key violation", e);
            }
            throw e;
        }
    }

    @Override
    public void putSentinels(Iterable<Cell> cells) {
        byte[] value = new byte[0];
        long ts = Value.INVALID_VALUE_TIMESTAMP;
        for (List<Cell> batch : Iterables.partition(Ordering.natural().immutableSortedCopy(cells), 1000)) {
            List<Object[]> args = Lists.newArrayListWithCapacity(batch.size());
            for (Cell cell : batch) {
                args.add(new Object[] {cell.getRowName(), cell.getColumnName(), ts, value,
                        cell.getRowName(), cell.getColumnName(), ts});
            }
            while (true) {
                try {
                    //        if (config.type().equals(OracleDdlConfig.TYPE)) {
                    //            return ((OracleDdlConfig) config)
                    //                    .tableNameMapper()
                    //                    .getShortPrefixedTableName(config.tablePrefix(), tableRef);
                    //        }
                    //        return config.tablePrefix() + tableRef;
                    //        if (config.type().equals(OracleDdlConfig.TYPE)) {
                    //            return ((OracleDdlConfig) config)
                    //                    .tableNameMapper()
                    //                    .getShortPrefixedTableName(config.tablePrefix(), tableRef);
                    //        }
                    //        return config.tablePrefix() + tableRef;
                    conns.get().insertManyUnregisteredQuery("/* INSERT_WHERE_NOT_EXISTS (" + tableRef + ") */"
                            + " INSERT INTO " + getInternalTableName() + " (row_name, col_name, ts, val) "
                            + " SELECT ?, ?, ?, ? FROM DUAL"
                            + " WHERE NOT EXISTS (SELECT * FROM " + getInternalTableName() + " WHERE"
                            + " row_name = ? AND"
                            + " col_name = ? AND"
                            + " ts = ?)",
                            args);
                    break;
                } catch (PalantirSqlException e) {
                    // we can't do atomic put if not exists, so retry if we get constraint violations
                    // TODO: Actually you can. Evaluate use of MERGE or UPSERT here.
                    if (!ExceptionCheck.isUniqueConstraintViolation(e)) {
                        throw e;
                    }
                }
            }
        }
    }

    @Override
    public void delete(List<Entry<Cell, Long>> entries) {
        List<Object[]> args = Lists.newArrayListWithCapacity(entries.size());
        for (Map.Entry<Cell, Long> entry : entries) {
            Cell cell = entry.getKey();
            args.add(new Object[] {cell.getRowName(), cell.getColumnName(), entry.getValue()});
        }

        conns.get().updateManyUnregisteredQuery(" /* DELETE_ONE (" + tableRef + ") */ "
                + " DELETE /*+ INDEX(m pk_" + getInternalTableName() + ") */ "
                + " FROM " + getInternalTableName() + " m "
                + " WHERE m.row_name = ? "
                + "  AND m.col_name = ? "
                + "  AND m.ts = ?",
                args);
    }

    private String getInternalTableName() {
        if (!config.type().equals(OracleDdlConfig.TYPE)) {
            return prefixedTableName();
        }
        AgnosticResultSet result = conns.get().selectResultSetUnregisteredQuery(
                String.format(
                        "SELECT short_table_name FROM %s WHERE table_name = ?",
                        AtlasDbConstants.ORACLE_NAME_MAPPING_TABLE),
                prefixedTableName());
        return result.get(0).getString("short_table_name");
    }

    private String prefixedTableName() {
        return config.tablePrefix() + DbKvs.internalTableName(tableRef);
    }

}
