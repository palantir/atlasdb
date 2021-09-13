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
package com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.TimestampRangeDelete;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleTableNameGetter;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbWriteTable;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.OraclePrefixedTableNames;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.OverflowMigrationState;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.UpdateExecutor;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.WhereClauses;
import com.palantir.common.exception.TableMappingNotFoundException;
import com.palantir.exception.PalantirSqlException;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.nexus.db.sql.ExceptionCheck;
import com.palantir.nexus.db.sql.SqlConnection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public final class OracleOverflowWriteTable implements DbWriteTable {
    private static final SafeLogger log = SafeLoggerFactory.get(OracleOverflowWriteTable.class);

    private final OracleDdlConfig config;
    private final ConnectionSupplier conns;
    private final OverflowSequenceSupplier overflowSequenceSupplier;
    private final OracleTableNameGetter oracleTableNameGetter;
    private final OraclePrefixedTableNames oraclePrefixedTableNames;
    private final TableReference tableRef;

    private OracleOverflowWriteTable(
            OracleDdlConfig config,
            ConnectionSupplier conns,
            OverflowSequenceSupplier sequenceSupplier,
            OracleTableNameGetter oracleTableNameGetter,
            OraclePrefixedTableNames oraclePrefixedTableNames,
            TableReference tableRef) {
        this.config = config;
        this.conns = conns;
        this.overflowSequenceSupplier = sequenceSupplier;
        this.oracleTableNameGetter = oracleTableNameGetter;
        this.oraclePrefixedTableNames = oraclePrefixedTableNames;
        this.tableRef = tableRef;
    }

    public static OracleOverflowWriteTable create(
            OracleDdlConfig config,
            ConnectionSupplier conns,
            OracleTableNameGetter oracleTableNameGetter,
            OraclePrefixedTableNames oraclePrefixedTableNames,
            TableReference tableRef) {
        OverflowSequenceSupplier sequenceSupplier = OverflowSequenceSupplier.create(conns, config.tablePrefix());
        return new OracleOverflowWriteTable(
                config, conns, sequenceSupplier, oracleTableNameGetter, oraclePrefixedTableNames, tableRef);
    }

    @Override
    public void put(Collection<Map.Entry<Cell, byte[]>> data, long ts) {
        List<Object[]> args = new ArrayList<>(data.size());
        List<Object[]> overflowArgs = new ArrayList<>();
        for (Map.Entry<Cell, byte[]> entry : data) {
            Cell cell = entry.getKey();
            byte[] val = entry.getValue();
            if (val.length <= AtlasDbConstants.ORACLE_OVERFLOW_THRESHOLD) {
                args.add(new Object[] {cell.getRowName(), cell.getColumnName(), ts, val, null});
            } else {
                long overflowId =
                        config.overflowIds().orElse(overflowSequenceSupplier).get();
                overflowArgs.add(new Object[] {overflowId, val});
                args.add(new Object[] {cell.getRowName(), cell.getColumnName(), ts, null, overflowId});
            }
        }
        put(args, overflowArgs);
    }

    @Override
    public void put(Collection<Map.Entry<Cell, Value>> data) {
        List<Object[]> args = new ArrayList<>(data.size());
        List<Object[]> overflowArgs = new ArrayList<>();
        for (Map.Entry<Cell, Value> entry : data) {
            Cell cell = entry.getKey();
            Value val = entry.getValue();
            if (val.getContents().length <= AtlasDbConstants.ORACLE_OVERFLOW_THRESHOLD) {
                args.add(new Object[] {
                    cell.getRowName(), cell.getColumnName(), val.getTimestamp(), val.getContents(), null
                });
            } else {
                long overflowId =
                        config.overflowIds().orElse(overflowSequenceSupplier).get();
                overflowArgs.add(new Object[] {overflowId, val.getContents()});
                args.add(new Object[] {cell.getRowName(), cell.getColumnName(), val.getTimestamp(), null, overflowId});
            }
        }
        put(args, overflowArgs);
    }

    private void put(List<Object[]> args, List<Object[]> overflowArgs) {
        if (!overflowArgs.isEmpty()) {
            if (config.overflowMigrationState() == OverflowMigrationState.UNSTARTED) {
                conns.get()
                        .insertManyUnregisteredQuery(
                                "/* INSERT_OVERFLOW */" + " INSERT INTO " + config.singleOverflowTable()
                                        + " (id, val) VALUES (?, ?) ",
                                overflowArgs);
            } else {
                String shortOverflowTableName = getShortOverflowTableName();
                conns.get()
                        .insertManyUnregisteredQuery(
                                "/* INSERT_OVERFLOW (" + shortOverflowTableName + ") */" + " INSERT INTO "
                                        + shortOverflowTableName + " (id, val) VALUES (?, ?) ",
                                overflowArgs);
            }
        }
        try {
            String shortTableName = oraclePrefixedTableNames.get(tableRef, conns);
            conns.get()
                    .insertManyUnregisteredQuery(
                            "/* INSERT_ONE (" + shortTableName + ") */"
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
        for (List<Cell> batch : Lists.partition(Ordering.natural().immutableSortedCopy(cells), 1000)) {
            List<Object[]> args = new ArrayList<>(batch.size());
            for (Cell cell : batch) {
                args.add(new Object[] {
                    cell.getRowName(),
                    cell.getColumnName(),
                    ts,
                    value,
                    null,
                    cell.getRowName(),
                    cell.getColumnName(),
                    ts
                });
            }
            while (true) {
                try {
                    String shortTableName = oraclePrefixedTableNames.get(tableRef, conns);
                    conns.get()
                            .insertManyUnregisteredQuery(
                                    "/* INSERT_WHERE_NOT_EXISTS (" + shortTableName + ") */"
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
    public void update(Cell cell, long ts, byte[] oldValue, byte[] newValue) {
        if (oldValue.length > AtlasDbConstants.ORACLE_OVERFLOW_THRESHOLD
                || newValue.length > AtlasDbConstants.ORACLE_OVERFLOW_THRESHOLD) {
            throw new UnsupportedOperationException(String.format(
                    "Update is not supported for values longer than the overflow threshold of %s bytes!",
                    AtlasDbConstants.ORACLE_OVERFLOW_THRESHOLD));
        }

        new UpdateExecutor(conns, tableRef, oraclePrefixedTableNames).update(cell, ts, oldValue, newValue);
    }

    @Override
    public void delete(List<Map.Entry<Cell, Long>> entries) {
        List<Object[]> args = new ArrayList<>(entries.size());
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
                        OverflowMigrationState.class,
                        config.overflowMigrationState().name());
        }
        String shortTableName = oraclePrefixedTableNames.get(tableRef, conns);
        SqlConnection conn = conns.get();
        try {
            log.debug(
                    "Got connection for delete on table {}: {}, autocommit={}",
                    UnsafeArg.of("shortTableName", shortTableName),
                    UnsafeArg.of("connection", conn.getUnderlyingConnection()),
                    SafeArg.of("autocommit", conn.getUnderlyingConnection().getAutoCommit()));
        } catch (PalantirSqlException | SQLException e) {
            //
        }
        conn.updateManyUnregisteredQuery(
                " /* DELETE_ONE (" + shortTableName + ") */ "
                        + " DELETE /*+ INDEX(m " + PrimaryKeyConstraintNames.get(shortTableName) + ") */ "
                        + " FROM " + shortTableName + " m "
                        + " WHERE m.row_name = ? "
                        + "  AND m.col_name = ? "
                        + "  AND m.ts = ?",
                args);
    }

    @Override
    public void delete(RangeRequest range) {
        String shortTableName = getShortTableName();

        switch (config.overflowMigrationState()) {
            case UNSTARTED:
                deleteOverflowRange(config.singleOverflowTable(), shortTableName, range);
                break;
            case IN_PROGRESS:
                deleteOverflowRange(config.singleOverflowTable(), shortTableName, range);
                deleteOverflowRange(getShortOverflowTableName(), shortTableName, range);
                break;
            case FINISHING:
            case FINISHED:
                deleteOverflowRange(getShortOverflowTableName(), shortTableName, range);
                break;
            default:
                throw new EnumConstantNotPresentException(
                        OverflowMigrationState.class,
                        config.overflowMigrationState().name());
        }

        // delete from main table
        StringBuilder query = new StringBuilder();
        query.append(" /* DELETE_RANGE (").append(shortTableName).append(") */ ");
        query.append(" DELETE /*+ INDEX(m pk_").append(shortTableName).append(") */ ");
        query.append(" FROM ").append(shortTableName).append(" m ");

        // add where clauses
        WhereClauses whereClauses = WhereClauses.create("m", range);

        List<Object> args = whereClauses.getArguments();
        List<String> clauses = whereClauses.getClauses();

        if (!clauses.isEmpty()) {
            query.append(" WHERE ");
            Joiner.on(" AND ").appendTo(query, clauses);
        }

        conns.get().updateUnregisteredQuery(query.toString(), args.toArray());
    }

    @Override
    public void deleteAllTimestamps(Map<Cell, TimestampRangeDelete> deletes) {
        List<Object[]> args = new ArrayList<>(deletes.size());
        deletes.forEach((cell, ts) -> args.add(new Object[] {
            cell.getRowName(), cell.getColumnName(),
            ts.minTimestampToDelete(), ts.maxTimestampToDelete()
        }));

        switch (config.overflowMigrationState()) {
            case UNSTARTED:
                deleteAllTimestampsOverflow(config.singleOverflowTable(), args);
                break;
            case IN_PROGRESS:
                deleteAllTimestampsOverflow(config.singleOverflowTable(), args);
                deleteAllTimestampsOverflow(getShortOverflowTableName(), args);
                break;
            case FINISHING: // fall through
            case FINISHED:
                deleteAllTimestampsOverflow(getShortOverflowTableName(), args);
                break;
            default:
                throw new EnumConstantNotPresentException(
                        OverflowMigrationState.class,
                        config.overflowMigrationState().name());
        }
        String shortTableName = oraclePrefixedTableNames.get(tableRef, conns);
        SqlConnection conn = conns.get();
        try {
            log.debug(
                    "Got connection for deleteAllTimestamps on table {}: {}, autocommit={}",
                    UnsafeArg.of("shortTableName", shortTableName),
                    UnsafeArg.of("connection", conn.getUnderlyingConnection()),
                    SafeArg.of("autocommit", conn.getUnderlyingConnection().getAutoCommit()));
        } catch (PalantirSqlException | SQLException e) {
            //
        }
        conn.updateManyUnregisteredQuery(
                " /* DELETE_ALL_TS (" + shortTableName + ") */ "
                        + " DELETE /*+ INDEX(m " + PrimaryKeyConstraintNames.get(shortTableName) + ") */ "
                        + " FROM " + shortTableName + " m "
                        + " WHERE m.row_name = ? "
                        + "  AND m.col_name = ? "
                        + "  AND m.ts >= ? "
                        + "  AND m.ts <= ?",
                args);
    }

    private void deleteOverflow(String overflowTable, List<Object[]> args) {
        String shortTableName = oraclePrefixedTableNames.get(tableRef, conns);
        conns.get()
                .updateManyUnregisteredQuery(
                        " /* DELETE_ONE_OVERFLOW (" + overflowTable + ") */ "
                                + " DELETE /*+ INDEX(m " + PrimaryKeyConstraintNames.get(overflowTable) + ") */ "
                                + "   FROM " + overflowTable + " m "
                                + "  WHERE m.id IN (SELECT /*+ INDEX(i " + PrimaryKeyConstraintNames.get(shortTableName)
                                + ") */ "
                                + "                        i.overflow "
                                + "                   FROM " + shortTableName + " i "
                                + "                  WHERE i.row_name = ? "
                                + "                    AND i.col_name = ? "
                                + "                    AND i.ts = ? "
                                + "                    AND i.overflow IS NOT NULL)",
                        args);
    }

    private void deleteOverflowRange(String overflowTable, String shortTableName, RangeRequest range) {
        StringBuilder query = new StringBuilder();
        query.append(" /* DELETE_RANGE_OVERFLOW (").append(overflowTable).append(") */ ");
        query.append(" DELETE /*+ INDEX(m pk_").append(overflowTable).append(") */ ");
        query.append("   FROM ").append(overflowTable).append(" m ");
        query.append(" WHERE m.id IN (");

        // subquery for finding rows in the short table
        query.append("SELECT /*+ INDEX(i pk_").append(shortTableName).append(") */ ");
        query.append("       i.overflow ");
        query.append("    FROM ").append(shortTableName).append(" i ");

        // add where clauses
        WhereClauses whereClauses = WhereClauses.create("i", range, "i.overflow IS NOT NULL");

        List<Object> args = whereClauses.getArguments();
        List<String> clauses = whereClauses.getClauses();

        if (!clauses.isEmpty()) {
            query.append(" WHERE ");
            Joiner.on(" AND ").appendTo(query, clauses);
        }

        query.append(")");

        // execute the query
        conns.get().updateUnregisteredQuery(query.toString(), args.toArray());
    }

    private void deleteAllTimestampsOverflow(String overflowTable, List<Object[]> args) {
        String shortTableName = oraclePrefixedTableNames.get(tableRef, conns);
        conns.get()
                .updateManyUnregisteredQuery(
                        " /* DELETE_ALL_TS_OVERFLOW (" + overflowTable + ") */ "
                                + " DELETE /*+ INDEX(m " + PrimaryKeyConstraintNames.get(overflowTable) + ") */ "
                                + "   FROM " + overflowTable + " m "
                                + "  WHERE m.id IN (SELECT /*+ INDEX(i " + PrimaryKeyConstraintNames.get(shortTableName)
                                + ") */ "
                                + "                        i.overflow "
                                + "                   FROM " + shortTableName + " i "
                                + "                  WHERE i.row_name = ? "
                                + "                    AND i.col_name = ? "
                                + "                    AND i.ts >= ? "
                                + "                    AND i.ts <= ? "
                                + "                    AND i.overflow IS NOT NULL)",
                        args);
    }

    private String getShortTableName() {
        try {
            return oracleTableNameGetter.getInternalShortTableName(conns, tableRef);
        } catch (TableMappingNotFoundException e) {
            throw Throwables.propagate(e);
        }
    }

    private String getShortOverflowTableName() {
        try {
            return oracleTableNameGetter.getInternalShortOverflowTableName(conns, tableRef);
        } catch (TableMappingNotFoundException e) {
            throw Throwables.propagate(e);
        }
    }
}
