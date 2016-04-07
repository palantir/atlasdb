package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.sql.ExceptionCheck;

public class SimpleDbWriteTable implements DbWriteTable {
    protected final String tableName;
    protected final ConnectionSupplier conns;

    public SimpleDbWriteTable(String tableName,
                              ConnectionSupplier conns) {
        this.tableName = tableName;
        this.conns = conns;
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
            conns.get().insertManyUnregisteredQuery(
                    "/* SQL_MET_INSERT_ONE (" + tableName + ") */" +
                    " INSERT INTO pt_met_" + tableName + " (row_name, col_name, ts, val) " +
                    " VALUES (?, ?, ?, ?) ",
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
                    conns.get().insertManyUnregisteredQuery(
                            "/* SQL_MET_INSERT_WHERE_NOT_EXISTS (" + tableName + ") */" +
                            " INSERT INTO pt_met_" + tableName + " (row_name, col_name, ts, val) " +
                            " SELECT ?, ?, ?, ? FROM DUAL" +
                            " WHERE NOT EXISTS (SELECT * FROM pt_met_" + tableName + " WHERE" +
                            " row_name = ? AND" +
                            " col_name = ? AND" +
                            " ts = ?)",
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
        conns.get().updateManyUnregisteredQuery(
                " /* SQL_MET_DELETE_ONE (" + tableName + ") */ " +
                " DELETE /*+ INDEX(m pk_pt_met_" + tableName + ") */ " +
                " FROM pt_met_" + tableName + " m " +
                " WHERE m.row_name = ? " +
                "  AND m.col_name = ? " +
                "  AND m.ts = ?",
                args);
    }
}
