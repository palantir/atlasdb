package com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbWriteTable;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.OverflowMigrationState;
import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.sql.ExceptionCheck;
import com.palantir.nexus.db.sql.PalantirSqlConnection;

public class OracleOverflowWriteTable implements DbWriteTable {
    private static final Logger log = LoggerFactory.getLogger(OracleOverflowWriteTable.class);

    private final String tableName;
    private final ConnectionSupplier conns;
    private final Supplier<Long> overflowIds;
    private final OverflowMigrationState migrationState;

    public OracleOverflowWriteTable(String tableName,
                                    ConnectionSupplier conns,
                                    Supplier<Long> overflowIds,
                                    OverflowMigrationState migrationState) {
        this.tableName = tableName;
        this.conns = conns;
        this.overflowIds = overflowIds;
        this.migrationState = migrationState;
    }

    @Override
    public void put(Collection<Map.Entry<Cell, byte[]>> data, long ts) {
        List<Object[]> args = Lists.newArrayListWithCapacity(data.size());
        List<Object[]> overflowArgs = Lists.newArrayList();
        for (Entry<Cell, byte[]> entry : data) {
            Cell cell = entry.getKey();
            byte[] val = entry.getValue();
            if (val.length <= 2000) {
                args.add(new Object[] { cell.getRowName(), cell.getColumnName(), ts, val, null });
            } else {
                long overflowId = overflowIds.get();
                overflowArgs.add(new Object[] { overflowId, val });
                args.add(new Object[] { cell.getRowName(), cell.getColumnName(), ts, null , overflowId });
            }
        }
        put(args, overflowArgs);
    }

    @Override
    public void put(Collection<Map.Entry<Cell, Value>> data) {
        List<Object[]> args = Lists.newArrayListWithCapacity(data.size());
        List<Object[]> overflowArgs = Lists.newArrayList();
        for (Entry<Cell, Value> entry : data) {
            Cell cell = entry.getKey();
            Value val = entry.getValue();
            if (val.getContents().length <= 2000) {
                args.add(new Object[] { cell.getRowName(), cell.getColumnName(), val.getTimestamp(), val.getContents(), null });
            } else {
                long overflowId = overflowIds.get();
                overflowArgs.add(new Object[] { overflowId, val.getContents() });
                args.add(new Object[] { cell.getRowName(), cell.getColumnName(), val.getTimestamp(), null , overflowId });
            }
        }
        put(args, overflowArgs);
    }

    private void put(List<Object[]> args, List<Object[]> overflowArgs) {
        if (!overflowArgs.isEmpty()) {
            if (migrationState == OverflowMigrationState.UNSTARTED) {
                conns.get().insertManyUnregisteredQuery(
                        "/* SQL_MET_INSERT_OVERFLOW */" +
                        " INSERT INTO pt_metropolis_overflow (id, val) VALUES (?, ?) ",
                        overflowArgs);
            } else {
                conns.get().insertManyUnregisteredQuery(
                        "/* SQL_MET_INSERT_OVERFLOW (" + tableName + ") */" +
                        " INSERT INTO pt_mo_" + tableName + " (id, val) VALUES (?, ?) ",
                        overflowArgs);
            }
        }
        try {
            conns.get().insertManyUnregisteredQuery(
                    "/* SQL_MET_INSERT_ONE (" + tableName + ") */" +
                    " INSERT INTO pt_met_" + tableName + " (row_name, col_name, ts, val, overflow) " +
                    " VALUES (?, ?, ?, ?, ?) ",
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
                args.add(new Object[] {cell.getRowName(), cell.getColumnName(), ts, value, null,
                        cell.getRowName(), cell.getColumnName(), ts});
            }
            while (true) {
                try {
                    conns.get().insertManyUnregisteredQuery(
                            "/* SQL_MET_INSERT_WHERE_NOT_EXISTS (" + tableName + ") */" +
                            " INSERT INTO pt_met_" + tableName + " (row_name, col_name, ts, val, overflow) " +
                            " SELECT ?, ?, ?, ?, ? FROM DUAL" +
                            " WHERE NOT EXISTS (SELECT * FROM pt_met_" + tableName + " WHERE" +
                            " row_name = ? AND" +
                            " col_name = ? AND" +
                            " ts = ?)",
                            args);
                    break;
                } catch (PalantirSqlException e) {
                    // we can't do atomic put if not exists, so retry if we get constraint violations
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
        switch (migrationState) {
        case UNSTARTED:
            deleteOverflow("pt_metropolis_overflow", args);
            break;
        case IN_PROGRESS:
            deleteOverflow("pt_metropolis_overflow", args);
            deleteOverflow("pt_mo_" + tableName, args);
            break;
        case FINISHING: // fall through
        case FINISHED:
            deleteOverflow("pt_mo_" + tableName, args);
            break;
        default:
            throw new EnumConstantNotPresentException(OverflowMigrationState.class, migrationState.name());
        }
        PalantirSqlConnection conn = conns.get();
        try {
            log.info("Got connection for delete on table {}: {}, autocommit={}", tableName, conn.getUnderlyingConnection(), conn.getUnderlyingConnection().getAutoCommit());
        } catch (PalantirSqlException e) {
            //
        } catch (SQLException e) {
            //
        }
        conn.updateManyUnregisteredQuery(
                " /* SQL_MET_DELETE_ONE (" + tableName + ") */ " +
                " DELETE /*+ INDEX(m pk_pt_met_" + tableName + ") */ " +
                " FROM pt_met_" + tableName + " m " +
                " WHERE m.row_name = ? " +
                "  AND m.col_name = ? " +
                "  AND m.ts = ?",
                args);
    }

    private void deleteOverflow(String overflowTable, List<Object[]> args) {
        conns.get().updateManyUnregisteredQuery(
                " /* SQL_MET_DELETE_ONE_OVERFLOW (" + tableName + ") */ " +
                " DELETE /*+ INDEX(m pk_" + overflowTable + ") */ " +
                "   FROM " + overflowTable + " m " +
                "  WHERE m.id IN (SELECT /*+ INDEX(i pk_pt_met_" + tableName + ") */ " +
                "                        i.overflow " +
                "                   FROM pt_met_" + tableName + " i " +
                "                  WHERE i.row_name = ? " +
                "                    AND i.col_name = ? " +
                "                    AND i.ts = ? " +
                "                    AND i.overflow IS NOT NULL)",
                args);
    }
}
