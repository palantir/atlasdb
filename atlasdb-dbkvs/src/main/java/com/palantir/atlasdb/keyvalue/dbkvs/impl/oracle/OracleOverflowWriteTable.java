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

import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.TableNameGetter;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbWriteTable;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.OverflowMigrationState;
import com.palantir.atlasdb.keyvalue.impl.TableMappingNotFoundException;
import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.sql.ExceptionCheck;
import com.palantir.nexus.db.sql.SqlConnection;

public final class OracleOverflowWriteTable implements DbWriteTable {
    private static final Logger log = LoggerFactory.getLogger(OracleOverflowWriteTable.class);

    private final OracleDdlConfig config;
    private final ConnectionSupplier conns;
    private final OverflowSequenceSupplier overflowSequenceSupplier;
    private final TableNameGetter tableNameGetter;
    private final TableReference tableRef;

    private OracleOverflowWriteTable(
            OracleDdlConfig config,
            ConnectionSupplier conns,
            OverflowSequenceSupplier sequenceSupplier,
            TableNameGetter tableNameGetter,
            TableReference tableRef) {
        this.config = config;
        this.conns = conns;
        this.overflowSequenceSupplier = sequenceSupplier;
        this.tableNameGetter = tableNameGetter;
        this.tableRef = tableRef;
    }

    public static OracleOverflowWriteTable create(
            OracleDdlConfig config,
            ConnectionSupplier conns,
            TableNameGetter tableNameGetter,
            TableReference tableRef) {
        OverflowSequenceSupplier sequenceSupplier = OverflowSequenceSupplier.create(conns, config.tablePrefix());
        return new OracleOverflowWriteTable(config, conns, sequenceSupplier, tableNameGetter, tableRef);
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
                long overflowId = config.overflowIds().orElse(overflowSequenceSupplier).get();
                overflowArgs.add(new Object[] { overflowId, val });
                args.add(new Object[] { cell.getRowName(), cell.getColumnName(), ts, null, overflowId });
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
                args.add(new Object[] {
                        cell.getRowName(), cell.getColumnName(), val.getTimestamp(), val.getContents(), null
                });
            } else {
                long overflowId = config.overflowIds().orElse(overflowSequenceSupplier).get();
                overflowArgs.add(new Object[] { overflowId, val.getContents() });
                args.add(new Object[] {
                        cell.getRowName(), cell.getColumnName(), val.getTimestamp(), null, overflowId
                });
            }
        }
        put(args, overflowArgs);
    }

    private void put(List<Object[]> args, List<Object[]> overflowArgs) {
        if (!overflowArgs.isEmpty()) {
            if (config.overflowMigrationState() == OverflowMigrationState.UNSTARTED) {
                conns.get().insertManyUnregisteredQuery("/* INSERT_OVERFLOW */"
                        + " INSERT INTO " + config.singleOverflowTable() + " (id, val) VALUES (?, ?) ",
                        overflowArgs);
            } else {
                String shortOverflowTableName = getShortOverflowTableName();
                conns.get().insertManyUnregisteredQuery(
                        "/* INSERT_OVERFLOW (" + shortOverflowTableName + ") */"
                        + " INSERT INTO " + shortOverflowTableName + " (id, val) VALUES (?, ?) ",
                        overflowArgs);
            }
        }
        try {
            String shortTableName = getShortTableName();
            conns.get().insertManyUnregisteredQuery("/* INSERT_ONE (" + shortTableName + ") */"
                    + " INSERT INTO " + shortTableName + " (row_name, col_name, ts, val, overflow) "
                    + " VALUES (?, ?, ?, ?, ?) ",
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
                    String shortTableName = getShortTableName();
                    conns.get().insertManyUnregisteredQuery("/* INSERT_WHERE_NOT_EXISTS (" + shortTableName + ") */"
                            + " INSERT INTO " + shortTableName
                            + "   (row_name, col_name, ts, val, overflow)"
                            + " SELECT ?, ?, ?, ?, ? FROM DUAL"
                            + " WHERE NOT EXISTS ("
                            + "   SELECT * FROM " + shortTableName
                            + "   WHERE row_name = ?"
                            + "     AND col_name = ?"
                            + "     AND ts = ?)",
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
                deleteOverflow(getShortOverflowTableName(), args);
                break;
            case FINISHING: // fall through
            case FINISHED:
                deleteOverflow(getShortOverflowTableName(), args);
                break;
            default:
                throw new EnumConstantNotPresentException(
                        OverflowMigrationState.class, config.overflowMigrationState().name());
        }
        SqlConnection conn = conns.get();
        try {
            log.info("Got connection for delete on table {}: {}, autocommit={}",
                    getShortTableName(),
                    conn.getUnderlyingConnection(),
                    conn.getUnderlyingConnection().getAutoCommit());
        } catch (PalantirSqlException | SQLException e) {
            //
        }
        conn.updateManyUnregisteredQuery(" /* DELETE_ONE (" + getShortTableName() + ") */ "
                + " DELETE /*+ INDEX(m pk_" + getShortTableName() + ") */ "
                + " FROM " + getShortTableName() + " m "
                + " WHERE m.row_name = ? "
                + "  AND m.col_name = ? "
                + "  AND m.ts = ?",
                args);
    }

    private void deleteOverflow(String overflowTable, List<Object[]> args) {
        String shortTableName = getShortTableName();
        conns.get().updateManyUnregisteredQuery(" /* DELETE_ONE_OVERFLOW (" + overflowTable + ") */ "
                + " DELETE /*+ INDEX(m pk_" + overflowTable + ") */ "
                + "   FROM " + overflowTable + " m "
                + "  WHERE m.id IN (SELECT /*+ INDEX(i pk_" + shortTableName + ") */ "
                + "                        i.overflow "
                + "                   FROM " + shortTableName + " i "
                + "                  WHERE i.row_name = ? "
                + "                    AND i.col_name = ? "
                + "                    AND i.ts = ? "
                + "                    AND i.overflow IS NOT NULL)",
                args);
    }

    private String getShortTableName() {
        try {
            return tableNameGetter.getInternalShortTableName(conns, tableRef);
        } catch (TableMappingNotFoundException e) {
            throw Throwables.propagate(e);
        }
    }

    private String getShortOverflowTableName() {
        try {
            return tableNameGetter.getInternalShortOverflowTableName(conns, tableRef);
        } catch (TableMappingNotFoundException e) {
            throw Throwables.propagate(e);
        }
    }
}
