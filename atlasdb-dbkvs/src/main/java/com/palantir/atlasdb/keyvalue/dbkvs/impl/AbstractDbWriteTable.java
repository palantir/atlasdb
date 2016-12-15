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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.dbkvs.DdlConfig;
import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.sql.BasicSQLUtils;
import com.palantir.nexus.db.sql.ExceptionCheck;
import com.palantir.nexus.db.sql.PalantirSqlConnection;

public abstract class AbstractDbWriteTable implements DbWriteTable {
    protected final DdlConfig config;
    protected final ConnectionSupplier conns;
    protected final TableReference tableRef;
    private final PrefixedTableNames prefixedTableNames;

    public AbstractDbWriteTable(
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
            String prefixedTableName = prefixedTableNames.get(tableRef);
            conns.get().insertManyUnregisteredQuery("/* INSERT_ONE (" + prefixedTableName + ") */"
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
        for (List<Cell> batch : Iterables.partition(Ordering.natural().immutableSortedCopy(cells), 1000)) {
            List<Object[]> args = Lists.newArrayListWithCapacity(batch.size());
            for (Cell cell : batch) {
                args.add(new Object[] {cell.getRowName(), cell.getColumnName(), ts, value,
                        cell.getRowName(), cell.getColumnName(), ts});
            }
            while (true) {
                try {
                    String prefixedTableName = prefixedTableNames.get(tableRef);
                    conns.get().insertManyUnregisteredQuery("/* INSERT_WHERE_NOT_EXISTS (" + prefixedTableName + ") */"
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
                    // TODO: Actually you can. Evaluate use of MERGE or UPSERT here.
                    if (!ExceptionCheck.isUniqueConstraintViolation(e)) {
                        throw e;
                    }
                }
            }
        }
    }

    @Override
    public void update(Cell cell, long ts, byte[] oldValue, byte[] newValue) {
        String prefixedTableName = prefixedTableNames.get(tableRef);
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
        String sqlString = "/* UPDATE (" + prefixedTableName + ") */"
                + " UPDATE " + prefixedTableName + ""
                + " SET row_name = ?, col_name = ?, ts = ?, val = ?"
                + " WHERE row_name = ?"
                + " AND col_name = ?"
                + " AND ts = ?"
                + " AND val = ?";
        int updated = ((PalantirSqlConnection) conns.get()).updateCountRowsUnregisteredQuery(sqlString,
                args);
        if (updated == 0) {
            // right now we don't know what's actually in the db :-(
            throw new CheckAndSetException(cell, tableRef, oldValue, ImmutableList.of());
        }
    }

    @Override
    public void delete(List<Entry<Cell, Long>> entries) {
        List<Object[]> args = Lists.newArrayListWithCapacity(entries.size());
        for (Map.Entry<Cell, Long> entry : entries) {
            Cell cell = entry.getKey();
            args.add(new Object[] {cell.getRowName(), cell.getColumnName(), entry.getValue()});
        }

        String prefixedTableName = prefixedTableNames.get(tableRef);
        conns.get().updateManyUnregisteredQuery(" /* DELETE_ONE (" + prefixedTableName + ") */ "
                + " DELETE /*+ INDEX(m pk_" + prefixedTableName + ") */ "
                + " FROM " + prefixedTableName + " m "
                + " WHERE m.row_name = ? "
                + "  AND m.col_name = ? "
                + "  AND m.ts = ?",
                args);
    }

    @Override
    public void delete(RangeRequest range) {
        String prefixedTableName = prefixedTableNames.get(tableRef);
        StringBuilder query = new StringBuilder();
        query.append(" /* DELETE_RANGE (").append(prefixedTableName).append(") */ ");
        query.append(" DELETE FROM ").append(prefixedTableName).append(" m ");

        // add where clauses to the query
        byte[] start = range.getStartInclusive();
        byte[] end = range.getEndExclusive();
        Collection<byte[]> cols = range.getColumnNames();
        List<Object> args = Lists.newArrayListWithCapacity(2 + cols.size());
        List<String> whereClauses = Lists.newArrayListWithCapacity(3);
        if (start.length > 0) {
            whereClauses.add(range.isReverse() ? "m.row_name <= ?" : "m.row_name >= ?");
            args.add(start);
        }
        if (end.length > 0) {
            whereClauses.add(range.isReverse() ? "m.row_name > ?" : "m.row_name < ?");
            args.add(end);
        }
        if (!cols.isEmpty()) {
            whereClauses.add("m.col_name IN (" + BasicSQLUtils.nArguments(cols.size()) + ")");
            args.addAll(cols);
        }

        if (!whereClauses.isEmpty()) {
            query.append(" WHERE ");
            Joiner.on(" AND ").appendTo(query, whereClauses);
        }

        // execute the query
        conns.get().updateUnregisteredQuery(query.toString(), args.toArray());
    }
}
