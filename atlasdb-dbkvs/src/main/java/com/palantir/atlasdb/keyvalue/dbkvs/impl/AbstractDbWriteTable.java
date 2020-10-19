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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.TimestampRangeDelete;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.dbkvs.DdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.PrimaryKeyConstraintNames;
import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.sql.ExceptionCheck;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public abstract class AbstractDbWriteTable implements DbWriteTable {
    protected final DdlConfig config;
    protected final ConnectionSupplier conns;
    protected final TableReference tableRef;
    private final PrefixedTableNames prefixedTableNames;

    protected AbstractDbWriteTable(
            DdlConfig config,
            ConnectionSupplier conns,
            TableReference tableRef,
            PrefixedTableNames prefixedTableNames) {
        this.config = config;
        this.conns = conns;
        this.tableRef = tableRef;
        this.prefixedTableNames = prefixedTableNames;
    }

    @Override
    public void put(Collection<Map.Entry<Cell, byte[]>> data, long ts) {
        List<Object[]> args = new ArrayList<>(data.size());
        for (Map.Entry<Cell, byte[]> entry : data) {
            Cell cell = entry.getKey();
            byte[] val = entry.getValue();
            args.add(new Object[] {cell.getRowName(), cell.getColumnName(), ts, val});
        }
        put(args);
    }

    @Override
    public void put(Collection<Map.Entry<Cell, Value>> data) {
        List<Object[]> args = new ArrayList<>(data.size());
        for (Map.Entry<Cell, Value> entry : data) {
            Cell cell = entry.getKey();
            Value val = entry.getValue();
            args.add(new Object[] {cell.getRowName(), cell.getColumnName(), val.getTimestamp(), val.getContents()});
        }
        put(args);
    }

    private void put(List<Object[]> args) {
        try {
            String prefixedTableName = prefixedTableNames.get(tableRef, conns);
            conns.get()
                    .insertManyUnregisteredQuery(
                            "/* INSERT_ONE (" + prefixedTableName + ") */"
                                    + " INSERT INTO " + prefixedTableName + " (row_name, col_name, ts, val) "
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
        for (List<Cell> batch : Lists.partition(Ordering.natural().immutableSortedCopy(cells), 1000)) {
            List<Object[]> args = new ArrayList<>(batch.size());
            for (Cell cell : batch) {
                args.add(new Object[] {
                    cell.getRowName(), cell.getColumnName(), ts, value, cell.getRowName(), cell.getColumnName(), ts
                });
            }
            while (true) {
                try {
                    String prefixedTableName = prefixedTableNames.get(tableRef, conns);
                    conns.get()
                            .insertManyUnregisteredQuery(
                                    "/* INSERT_WHERE_NOT_EXISTS (" + prefixedTableName + ") */"
                                            + " INSERT INTO " + prefixedTableName + " (row_name, col_name, ts, val) "
                                            + " SELECT ?, ?, ?, ? FROM DUAL"
                                            + " WHERE NOT EXISTS (SELECT * FROM " + prefixedTableName + " WHERE"
                                            + " row_name = ? AND"
                                            + " col_name = ? AND"
                                            + " ts = ?)",
                                    args);
                    break;
                } catch (PalantirSqlException e) {
                    // we can't do atomic put if not exists, so retry if we get constraint violations
                    // TODO(jboreiko): Actually you can. Evaluate use of MERGE or UPSERT here.
                    if (!ExceptionCheck.isUniqueConstraintViolation(e)) {
                        throw e;
                    }
                }
            }
        }
    }

    @Override
    public void update(Cell cell, long ts, byte[] oldValue, byte[] newValue) {
        new UpdateExecutor(conns, tableRef, prefixedTableNames).update(cell, ts, oldValue, newValue);
    }

    @Override
    public void delete(List<Map.Entry<Cell, Long>> entries) {
        List<Object[]> args = new ArrayList<>(entries.size());
        for (Map.Entry<Cell, Long> entry : entries) {
            Cell cell = entry.getKey();
            args.add(new Object[] {cell.getRowName(), cell.getColumnName(), entry.getValue()});
        }

        String prefixedTableName = prefixedTableNames.get(tableRef, conns);
        conns.get()
                .updateManyUnregisteredQuery(
                        " /* DELETE_ONE (" + prefixedTableName + ") */ "
                                + " DELETE /*+ INDEX(m " + PrimaryKeyConstraintNames.get(prefixedTableName) + ") */ "
                                + " FROM " + prefixedTableName + " m "
                                + " WHERE m.row_name = ? "
                                + "  AND m.col_name = ? "
                                + "  AND m.ts = ?",
                        args);
    }

    @Override
    public void delete(RangeRequest range) {
        String prefixedTableName = prefixedTableNames.get(tableRef, conns);
        StringBuilder query = new StringBuilder();
        query.append(" /* DELETE_RANGE (").append(prefixedTableName).append(") */ ");
        query.append(" DELETE FROM ").append(prefixedTableName).append(" m ");

        WhereClauses whereClauses = WhereClauses.create("m", range);

        List<Object> args = whereClauses.getArguments();
        List<String> clauses = whereClauses.getClauses();

        if (!clauses.isEmpty()) {
            query.append(" WHERE ");
            Joiner.on(" AND ").appendTo(query, clauses);
        }

        // execute the query
        conns.get().updateUnregisteredQuery(query.toString(), args.toArray());
    }

    @Override
    public void deleteAllTimestamps(Map<Cell, TimestampRangeDelete> deletes) {
        List<Object[]> args = new ArrayList<>(deletes.size());
        deletes.forEach((cell, ts) -> args.add(new Object[] {
            cell.getRowName(), cell.getColumnName(),
            ts.minTimestampToDelete(), ts.maxTimestampToDelete()
        }));

        String prefixedTableName = prefixedTableNames.get(tableRef, conns);
        conns.get()
                .updateManyUnregisteredQuery(
                        " /* DELETE_ALL_TS (" + prefixedTableName + ") */ "
                                + " DELETE /*+ INDEX(m " + PrimaryKeyConstraintNames.get(prefixedTableName) + ") */ "
                                + " FROM " + prefixedTableName + " m "
                                + " WHERE m.row_name = ? "
                                + "  AND m.col_name = ? "
                                + "  AND m.ts >= ? "
                                + "  AND m.ts <= ?",
                        args);
    }
}
