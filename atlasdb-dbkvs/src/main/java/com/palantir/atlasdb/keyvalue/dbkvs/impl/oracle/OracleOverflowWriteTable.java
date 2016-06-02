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
package com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbWriteTable;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.OverflowMigrationState;
import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.sql.ExceptionCheck;
import com.palantir.nexus.db.sql.SqlConnection;

public class OracleOverflowWriteTable implements DbWriteTable {
    private static final Logger log = LoggerFactory.getLogger(OracleOverflowWriteTable.class);

    private final String tableName;
    private final ConnectionSupplier conns;
    private final OracleKeyValueServiceConfig config;

    public OracleOverflowWriteTable(String tableName,
                                    ConnectionSupplier conns,
                                    OracleKeyValueServiceConfig config) {
        this.tableName = tableName;
        this.conns = conns;
        this.config = config;
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
                long overflowId = config.overflowIds().get();
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
                long overflowId = config.overflowIds().get();
                overflowArgs.add(new Object[] { overflowId, val.getContents() });
                args.add(new Object[] { cell.getRowName(), cell.getColumnName(), val.getTimestamp(), null , overflowId });
            }
        }
        put(args, overflowArgs);
    }

    private void put(List<Object[]> args, List<Object[]> overflowArgs) {
        if (!overflowArgs.isEmpty()) {
            if (config.overflowMigrationState() == OverflowMigrationState.UNSTARTED) {
                conns.get().insertManyUnregisteredQuery(
                        "/* INSERT_OVERFLOW */" +
                        " INSERT INTO " + config.singleOverflowTable() + " (id, val) VALUES (?, ?) ",
                        overflowArgs);
            } else {
                conns.get().insertManyUnregisteredQuery(
                        "/* INSERT_OVERFLOW (" + tableName + ") */" +
                        " INSERT INTO " + prefixedOverflowTableName() + " (id, val) VALUES (?, ?) ",
                        overflowArgs);
            }
        }
        try {
            conns.get().insertManyUnregisteredQuery(
                    "/* INSERT_ONE (" + tableName + ") */" +
                    " INSERT INTO " + prefixedTableName() + " (row_name, col_name, ts, val, overflow) " +
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
                            "/* INSERT_WHERE_NOT_EXISTS (" + tableName + ") */" +
                            " INSERT INTO " + prefixedTableName() + " (row_name, col_name, ts, val, overflow) " +
                            " SELECT ?, ?, ?, ?, ? FROM DUAL" +
                            " WHERE NOT EXISTS (SELECT * FROM " + prefixedTableName() + " WHERE" +
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
        switch (config.overflowMigrationState()) {
        case UNSTARTED:
            deleteOverflow(config.singleOverflowTable(), args);
            break;
        case IN_PROGRESS:
            deleteOverflow(config.singleOverflowTable(), args);
            deleteOverflow(prefixedOverflowTableName(), args);
            break;
        case FINISHING: // fall through
        case FINISHED:
            deleteOverflow(prefixedOverflowTableName(), args);
            break;
        default:
            throw new EnumConstantNotPresentException(OverflowMigrationState.class, config.overflowMigrationState().name());
        }
        SqlConnection conn = conns.get();
        try {
            log.info("Got connection for delete on table {}: {}, autocommit={}", tableName, conn.getUnderlyingConnection(), conn.getUnderlyingConnection().getAutoCommit());
        } catch (PalantirSqlException e) {
            //
        } catch (SQLException e) {
            //
        }
        conn.updateManyUnregisteredQuery(
                " /* DELETE_ONE (" + tableName + ") */ " +
                " DELETE /*+ INDEX(m pk_" + prefixedTableName() + ") */ " +
                " FROM " + prefixedTableName() + " m " +
                " WHERE m.row_name = ? " +
                "  AND m.col_name = ? " +
                "  AND m.ts = ?",
                args);
    }

    private void deleteOverflow(String overflowTable, List<Object[]> args) {
        conns.get().updateManyUnregisteredQuery(
                " /* DELETE_ONE_OVERFLOW (" + tableName + ") */ " +
                " DELETE /*+ INDEX(m pk_" + overflowTable + ") */ " +
                "   FROM " + overflowTable + " m " +
                "  WHERE m.id IN (SELECT /*+ INDEX(i pk_" + prefixedTableName() + ") */ " +
                "                        i.overflow " +
                "                   FROM " + prefixedTableName() + " i " +
                "                  WHERE i.row_name = ? " +
                "                    AND i.col_name = ? " +
                "                    AND i.ts = ? " +
                "                    AND i.overflow IS NOT NULL)",
                args);
    }

    private String prefixedTableName() {
        return config.shared().tablePrefix() + tableName;
    }

    private String prefixedOverflowTableName() {
        return config.shared().tablePrefix() + tableName;
    }
}
