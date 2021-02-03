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
package com.palantir.atlasdb.keyvalue.jdbc;

import static com.palantir.atlasdb.keyvalue.jdbc.impl.JdbcConstants.ATLAS_TABLE;
import static com.palantir.atlasdb.keyvalue.jdbc.impl.JdbcConstants.A_COL_NAME;
import static com.palantir.atlasdb.keyvalue.jdbc.impl.JdbcConstants.A_ROW_NAME;
import static com.palantir.atlasdb.keyvalue.jdbc.impl.JdbcConstants.A_TIMESTAMP;
import static com.palantir.atlasdb.keyvalue.jdbc.impl.JdbcConstants.A_VALUE;
import static com.palantir.atlasdb.keyvalue.jdbc.impl.JdbcConstants.COL_NAME;
import static com.palantir.atlasdb.keyvalue.jdbc.impl.JdbcConstants.MAX_TIMESTAMP;
import static com.palantir.atlasdb.keyvalue.jdbc.impl.JdbcConstants.METADATA;
import static com.palantir.atlasdb.keyvalue.jdbc.impl.JdbcConstants.RANGE_TABLE;
import static com.palantir.atlasdb.keyvalue.jdbc.impl.JdbcConstants.ROW_NAME;
import static com.palantir.atlasdb.keyvalue.jdbc.impl.JdbcConstants.R_ROW_NAME;
import static com.palantir.atlasdb.keyvalue.jdbc.impl.JdbcConstants.R_TIMESTAMP;
import static com.palantir.atlasdb.keyvalue.jdbc.impl.JdbcConstants.T1_COL_NAME;
import static com.palantir.atlasdb.keyvalue.jdbc.impl.JdbcConstants.T1_ROW_NAME;
import static com.palantir.atlasdb.keyvalue.jdbc.impl.JdbcConstants.T1_TIMESTAMP;
import static com.palantir.atlasdb.keyvalue.jdbc.impl.JdbcConstants.T1_VALUE;
import static com.palantir.atlasdb.keyvalue.jdbc.impl.JdbcConstants.T2_COL_NAME;
import static com.palantir.atlasdb.keyvalue.jdbc.impl.JdbcConstants.T2_MAX_TIMESTAMP;
import static com.palantir.atlasdb.keyvalue.jdbc.impl.JdbcConstants.T2_ROW_NAME;
import static com.palantir.atlasdb.keyvalue.jdbc.impl.JdbcConstants.TABLE_NAME;
import static com.palantir.atlasdb.keyvalue.jdbc.impl.JdbcConstants.TEMP_TABLE_1;
import static com.palantir.atlasdb.keyvalue.jdbc.impl.JdbcConstants.TEMP_TABLE_2;
import static com.palantir.atlasdb.keyvalue.jdbc.impl.JdbcConstants.TIMESTAMP;
import static com.palantir.atlasdb.keyvalue.jdbc.impl.JdbcConstants.VALUE;
import static org.jooq.Clause.TABLE_VALUES;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.row;
import static org.jooq.impl.DSL.table;
import static org.jooq.impl.SQLDataType.BIGINT;
import static org.jooq.impl.SQLDataType.BLOB;
import static org.jooq.impl.SQLDataType.VARBINARY;
import static org.jooq.impl.SQLDataType.VARCHAR;

import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.base.Throwables;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.UnsignedBytes;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.jdbc.config.JdbcDataSourceConfiguration;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetCompatibility;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.ClusterAvailabilityStatus;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.TimestampRangeDelete;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.GetCandidateCellsForSweepingShim;
import com.palantir.atlasdb.keyvalue.impl.KeyValueServices;
import com.palantir.atlasdb.keyvalue.jdbc.impl.MultiTimestampPutBatch;
import com.palantir.atlasdb.keyvalue.jdbc.impl.PutBatch;
import com.palantir.atlasdb.keyvalue.jdbc.impl.SingleTimestampPutBatch;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.util.paging.AbstractPagingIterable;
import com.palantir.util.paging.SimpleTokenBackedResultsPage;
import com.palantir.util.paging.TokenBackedBasicResultsPage;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import javax.sql.DataSource;
import org.jooq.BatchBindStep;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.InsertValuesStep4;
import org.jooq.Query;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.RenderContext;
import org.jooq.Result;
import org.jooq.Row;
import org.jooq.Row3;
import org.jooq.RowN;
import org.jooq.SQLDialect;
import org.jooq.Select;
import org.jooq.SelectField;
import org.jooq.SelectOffsetStep;
import org.jooq.Table;
import org.jooq.TableLike;
import org.jooq.conf.RenderNameStyle;
import org.jooq.conf.Settings;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

public final class JdbcKeyValueService implements KeyValueService {
    private final int rowBatchSize;
    private final int batchSizeForReads;
    private final int batchSizeForMutations;
    private final String tablePrefix;
    private final SQLDialect sqlDialect;
    private final DataSource dataSource;

    private final Settings settings;
    public final Table<Record> METADATA_TABLE;

    private JdbcKeyValueService(
            Settings settings,
            SQLDialect sqlDialect,
            DataSource dataSource,
            String tablePrefix,
            int rowBatchSize,
            int batchSizeForReads,
            int batchSizeForMutations) {
        this.settings = settings;
        this.sqlDialect = sqlDialect;
        this.dataSource = dataSource;
        this.tablePrefix = tablePrefix;
        this.rowBatchSize = rowBatchSize;
        this.batchSizeForReads = batchSizeForReads;
        this.batchSizeForMutations = batchSizeForMutations;

        METADATA_TABLE = table(tablePrefix + "_metadata");
    }

    public static JdbcKeyValueService create(JdbcKeyValueConfiguration config) {
        JdbcDataSourceConfiguration dataSourceConfig = config.getDataSourceConfig();
        SQLDialect sqlDialect = SQLDialect.valueOf(dataSourceConfig.getSqlDialect());
        DataSource dataSource = dataSourceConfig.createDataSource();
        Settings settings = new Settings();
        settings.setRenderNameStyle(RenderNameStyle.AS_IS);
        final JdbcKeyValueService kvs = new JdbcKeyValueService(
                settings,
                sqlDialect,
                dataSource,
                config.getTablePrefix(),
                config.getRowBatchSize(),
                config.getBatchSizeForReads(),
                config.getBatchSizeForMutations());

        kvs.run((Function<DSLContext, Void>) ctx -> {
            String partialSql = ctx.createTable(kvs.METADATA_TABLE)
                    .column(TABLE_NAME, VARCHAR.nullable(false))
                    .column(METADATA, BLOB.nullable(false))
                    .getSQL();
            int endIndex = partialSql.lastIndexOf(')');
            String fullSql = partialSql.substring(0, endIndex) + "," + " CONSTRAINT pk_"
                    + kvs.METADATA_TABLE.getName() + " PRIMARY KEY ("
                    + TABLE_NAME.getName() + ")" + partialSql.substring(endIndex);
            try {
                ctx.execute(fullSql);
            } catch (DataAccessException e) {
                kvs.handleTableCreationException(e);
            }
            return null;
        });

        return kvs;
    }

    @Override
    public Collection<? extends KeyValueService> getDelegates() {
        return ImmutableList.of();
    }

    @Override
    public Map<Cell, Value> getRows(
            TableReference tableRef, Iterable<byte[]> rows, ColumnSelection columnSelection, long timestamp) {
        HashMap<Cell, Value> ret = new HashMap<>();
        for (List<byte[]> part : Iterables.partition(rows, rowBatchSize)) {
            ret.putAll(getRowsPartition(tableRef, part, columnSelection, timestamp));
        }
        return ret;
    }

    private Map<Cell, Value> getRowsPartition(
            TableReference tableRef, List<byte[]> rows, ColumnSelection columnSelection, long timestamp) {
        if (columnSelection.allColumnsSelected()) {
            return getRowsAllColumns(tableRef, rows, timestamp);
        } else {
            return getRowsSomeColumns(tableRef, rows, columnSelection, timestamp);
        }
    }

    private Map<Cell, Value> getRowsAllColumns(
            final TableReference tableRef, final Iterable<byte[]> rows, final long timestamp) {
        if (Iterables.isEmpty(rows)) {
            return ImmutableMap.of();
        }

        return run(ctx -> {
            Select<? extends Record> query =
                    getLatestTimestampQueryAllColumns(ctx, tableRef, ImmutableList.copyOf(rows), timestamp);
            Result<? extends Record> records = fetchValues(ctx, tableRef, query);
            Map<Cell, Value> results = Maps.newHashMapWithExpectedSize(records.size());
            for (Record record : records) {
                results.put(
                        Cell.create(record.getValue(A_ROW_NAME), record.getValue(A_COL_NAME)),
                        Value.create(record.getValue(A_VALUE), record.getValue(A_TIMESTAMP)));
            }
            return results;
        });
    }

    private Map<Cell, Value> getRowsSomeColumns(
            final TableReference tableRef,
            final Iterable<byte[]> rows,
            final ColumnSelection columnSelection,
            final long timestamp) {
        if (Iterables.isEmpty(rows)) {
            return ImmutableMap.of();
        }

        return run(ctx -> {
            Select<? extends Record> query = getLatestTimestampQuerySomeColumns(
                    ctx, tableRef, ImmutableList.copyOf(rows), columnSelection.getSelectedColumns(), timestamp);
            Result<? extends Record> records = fetchValues(ctx, tableRef, query);
            Map<Cell, Value> results = Maps.newHashMapWithExpectedSize(records.size());
            for (Record record : records) {
                results.put(
                        Cell.create(record.getValue(A_ROW_NAME), record.getValue(A_COL_NAME)),
                        Value.create(record.getValue(A_VALUE), record.getValue(A_TIMESTAMP)));
            }
            return results;
        });
    }

    @Override
    public Map<Cell, Value> get(final TableReference tableRef, final Map<Cell, Long> timestampByCell) {
        if (timestampByCell.isEmpty()) {
            return ImmutableMap.of();
        }

        Map<Cell, Value> toReturn = new HashMap<>();
        for (List<Map.Entry<Cell, Long>> partition :
                Iterables.partition(timestampByCell.entrySet(), batchSizeForReads)) {
            toReturn.putAll(run(ctx -> {
                Select<? extends Record> query =
                        getLatestTimestampQueryManyTimestamps(ctx, tableRef, toRows(partition));
                Result<? extends Record> records = fetchValues(ctx, tableRef, query);
                Map<Cell, Value> results = Maps.newHashMapWithExpectedSize(records.size());
                for (Record record : records) {
                    results.put(
                            Cell.create(record.getValue(A_ROW_NAME), record.getValue(A_COL_NAME)),
                            Value.create(record.getValue(A_VALUE), record.getValue(A_TIMESTAMP)));
                }
                return results;
            }));
        }
        return toReturn;
    }

    @Override
    public Map<Cell, Long> getLatestTimestamps(final TableReference tableRef, final Map<Cell, Long> timestampByCell) {
        if (timestampByCell.isEmpty()) {
            return ImmutableMap.of();
        }

        Map<Cell, Long> toReturn = new HashMap<>();
        for (List<Map.Entry<Cell, Long>> partition :
                Iterables.partition(timestampByCell.entrySet(), batchSizeForReads)) {
            toReturn.putAll(run(ctx -> {
                Select<? extends Record> query =
                        getLatestTimestampQueryManyTimestamps(ctx, tableRef, toRows(partition));
                Result<? extends Record> records = query.fetch();
                Map<Cell, Long> results = Maps.newHashMapWithExpectedSize(records.size());
                for (Record record : records) {
                    results.put(
                            Cell.create(record.getValue(A_ROW_NAME), record.getValue(A_COL_NAME)),
                            record.getValue(MAX_TIMESTAMP, Long.class));
                }
                return results;
            }));
        }
        return toReturn;
    }

    @Override
    public Multimap<Cell, Long> getAllTimestamps(
            final TableReference tableRef, final Set<Cell> cells, final long timestamp)
            throws InsufficientConsistencyException {
        if (cells.isEmpty()) {
            return ImmutableMultimap.of();
        }

        Multimap<Cell, Long> toReturn = ArrayListMultimap.create();
        for (List<Cell> partition : Iterables.partition(cells, batchSizeForReads)) {
            toReturn.putAll(run(ctx -> {
                Result<? extends Record> records = ctx.select(A_ROW_NAME, A_COL_NAME, A_TIMESTAMP)
                        .from(atlasTable(tableRef).as(ATLAS_TABLE))
                        .join(values(ctx, toRows(new HashSet<>(partition)), TEMP_TABLE_1, ROW_NAME, COL_NAME))
                        .on(A_ROW_NAME.eq(T1_ROW_NAME).and(A_COL_NAME.eq(T1_COL_NAME)))
                        .where(A_TIMESTAMP.lessThan(timestamp))
                        .fetch();
                Multimap<Cell, Long> results = ArrayListMultimap.create(records.size() / 4, 4);
                for (Record record : records) {
                    results.put(
                            Cell.create(record.getValue(A_ROW_NAME), record.getValue(A_COL_NAME)),
                            record.getValue(A_TIMESTAMP));
                }
                return results;
            }));
        }
        return toReturn;
    }

    private static RowN[] toRows(Set<Cell> cells) {
        RowN[] rows = new RowN[cells.size()];
        int i = 0;
        for (Cell cell : cells) {
            rows[i++] = row(new Object[] {cell.getRowName(), cell.getColumnName()});
        }
        return rows;
    }

    private static RowN[] toRows(List<Map.Entry<Cell, Long>> cellTimestampPairs) {
        RowN[] rows = new RowN[cellTimestampPairs.size()];
        int i = 0;
        for (Map.Entry<Cell, Long> entry : cellTimestampPairs) {
            rows[i++] = row(
                    new Object[] {entry.getKey().getRowName(), entry.getKey().getColumnName(), entry.getValue()});
        }
        return rows;
    }

    private Select<? extends Record> getLatestTimestampQueryAllColumns(
            DSLContext ctx, TableReference tableRef, Collection<byte[]> rows, long timestamp) {
        return ctx.select(A_ROW_NAME, A_COL_NAME, DSL.max(A_TIMESTAMP).as(MAX_TIMESTAMP))
                .from(atlasTable(tableRef).as(ATLAS_TABLE))
                .where(A_ROW_NAME.in(rows).and(A_TIMESTAMP.lessThan(timestamp)))
                .groupBy(A_ROW_NAME, A_COL_NAME);
    }

    private Select<? extends Record> getLatestTimestampQueryAllColumnsSubQuery(
            DSLContext ctx, TableReference tableRef, Select<Record1<byte[]>> subQuery, long timestamp) {
        return ctx.select(A_ROW_NAME, A_COL_NAME, DSL.max(A_TIMESTAMP).as(MAX_TIMESTAMP))
                .from(atlasTable(tableRef).as(ATLAS_TABLE))
                .where(A_ROW_NAME.in(subQuery).and(A_TIMESTAMP.lessThan(timestamp)))
                .groupBy(A_ROW_NAME, A_COL_NAME);
    }

    private Select<? extends Record> getLatestTimestampQuerySomeColumnsSubQuery(
            DSLContext ctx,
            TableReference tableRef,
            Select<Record1<byte[]>> subQuery,
            Collection<byte[]> cols,
            long timestamp) {
        return ctx.select(A_ROW_NAME, A_COL_NAME, DSL.max(A_TIMESTAMP).as(MAX_TIMESTAMP))
                .from(atlasTable(tableRef).as(ATLAS_TABLE))
                .where(A_ROW_NAME.in(subQuery).and(A_COL_NAME.in(cols)))
                .and(A_TIMESTAMP.lessThan(timestamp))
                .groupBy(A_ROW_NAME, A_COL_NAME);
    }

    private Select<? extends Record> getLatestTimestampQuerySomeColumns(
            DSLContext ctx, TableReference tableRef, Collection<byte[]> rows, Collection<byte[]> cols, long timestamp) {
        return ctx.select(A_ROW_NAME, A_COL_NAME, DSL.max(A_TIMESTAMP).as(MAX_TIMESTAMP))
                .from(atlasTable(tableRef).as(ATLAS_TABLE))
                .where(A_ROW_NAME.in(rows).and(A_COL_NAME.in(cols)))
                .and(A_TIMESTAMP.lessThan(timestamp))
                .groupBy(A_ROW_NAME, A_COL_NAME);
    }

    private Select<? extends Record> getAllTimestampsQueryAllColumns(
            DSLContext ctx, TableReference tableRef, Select<Record1<byte[]>> subQuery, long timestamp) {
        return ctx.select(A_ROW_NAME, A_COL_NAME, A_TIMESTAMP)
                .from(atlasTable(tableRef).as(ATLAS_TABLE))
                .where(A_ROW_NAME.in(subQuery).and(A_TIMESTAMP.lessThan(timestamp)));
    }

    private Select<? extends Record> getAllTimestampsQuerySomeColumns(
            DSLContext ctx,
            TableReference tableRef,
            Select<Record1<byte[]>> subQuery,
            Collection<byte[]> cols,
            long timestamp) {
        return ctx.select(A_ROW_NAME, A_COL_NAME, A_TIMESTAMP)
                .from(atlasTable(tableRef).as(ATLAS_TABLE))
                .where(A_ROW_NAME.in(subQuery).and(A_COL_NAME.in(cols)))
                .and(A_TIMESTAMP.lessThan(timestamp));
    }

    private Select<? extends Record> getLatestTimestampQueryManyTimestamps(
            DSLContext ctx, TableReference tableRef, RowN[] rows) {
        return ctx.select(A_ROW_NAME, A_COL_NAME, DSL.max(A_TIMESTAMP).as(MAX_TIMESTAMP))
                .from(atlasTable(tableRef).as(ATLAS_TABLE))
                .join(values(ctx, rows, TEMP_TABLE_1, ROW_NAME, COL_NAME, TIMESTAMP))
                .on(A_ROW_NAME.eq(T1_ROW_NAME).and(A_COL_NAME.eq(T1_COL_NAME)))
                .where(A_TIMESTAMP.lessThan(T1_TIMESTAMP))
                .groupBy(A_ROW_NAME, A_COL_NAME);
    }

    private Result<? extends Record> fetchValues(
            DSLContext ctx, TableReference tableRef, Select<? extends Record> subQuery) {
        return ctx.select(A_ROW_NAME, A_COL_NAME, A_TIMESTAMP, A_VALUE)
                .from(atlasTable(tableRef).as(ATLAS_TABLE))
                .join(subQuery.asTable(TEMP_TABLE_2))
                .on(A_ROW_NAME.eq(T2_ROW_NAME).and(A_COL_NAME.eq(T2_COL_NAME)).and(A_TIMESTAMP.eq(T2_MAX_TIMESTAMP)))
                .fetch();
    }

    @Override
    public void put(final TableReference tableRef, final Map<Cell, byte[]> values, final long timestamp)
            throws KeyAlreadyExistsException {
        if (values.isEmpty()) {
            return;
        }

        for (List<Map.Entry<Cell, byte[]>> partition : Iterables.partition(values.entrySet(), batchSizeForMutations)) {
            run((Function<DSLContext, Void>) ctx -> {
                putBatch(ctx, tableRef, SingleTimestampPutBatch.create(partition, timestamp), true);
                return null;
            });
        }
    }

    @Override
    public void multiPut(final Map<TableReference, ? extends Map<Cell, byte[]>> valuesByTable, final long timestamp)
            throws KeyAlreadyExistsException {
        run((Function<DSLContext, Void>) ctx -> {
            for (Map.Entry<TableReference, ? extends Map<Cell, byte[]>> entry : valuesByTable.entrySet()) {
                TableReference tableRef = entry.getKey();
                Map<Cell, byte[]> values = entry.getValue();
                if (!values.isEmpty()) {
                    for (List<Map.Entry<Cell, byte[]>> partition :
                            Iterables.partition(values.entrySet(), batchSizeForMutations)) {
                        putBatch(ctx, tableRef, SingleTimestampPutBatch.create(partition, timestamp), true);
                    }
                }
            }
            return null;
        });
    }

    @Override
    public void putWithTimestamps(final TableReference tableRef, final Multimap<Cell, Value> values)
            throws KeyAlreadyExistsException {
        if (values.isEmpty()) {
            return;
        }

        for (List<Map.Entry<Cell, Value>> partValues : Iterables.partition(values.entries(), batchSizeForMutations)) {
            run((Function<DSLContext, Void>) ctx -> {
                putBatch(ctx, tableRef, new MultiTimestampPutBatch(partValues), true);
                return null;
            });
        }
    }

    @Override
    public void putUnlessExists(final TableReference tableRef, final Map<Cell, byte[]> values)
            throws KeyAlreadyExistsException {
        if (values.isEmpty()) {
            return;
        }
        for (List<Map.Entry<Cell, byte[]>> partValues : Iterables.partition(values.entrySet(), batchSizeForMutations)) {
            run((Function<DSLContext, Void>) ctx -> {
                putBatch(ctx, tableRef, SingleTimestampPutBatch.create(partValues, 0L), false);
                return null;
            });
        }
    }

    @Override
    public CheckAndSetCompatibility getCheckAndSetCompatibility() {
        return CheckAndSetCompatibility.NOT_SUPPORTED;
    }

    @Override
    public void checkAndSet(CheckAndSetRequest checkAndSetRequest) {
        throw new UnsupportedOperationException("Check and set is not supported for JDBC KVS");
    }

    @Override
    public void addGarbageCollectionSentinelValues(final TableReference tableRef, Iterable<Cell> cells) {
        int numCells = Iterables.size(cells);
        if (numCells == 0) {
            return;
        }
        for (List<Cell> partCells : Iterables.partition(cells, batchSizeForMutations)) {
            Long timestamp = Value.INVALID_VALUE_TIMESTAMP;
            byte[] value = new byte[0];
            final RowN[] rows = new RowN[numCells];
            int i = 0;
            for (Cell cell : partCells) {
                rows[i++] = row(new Object[] {cell.getRowName(), cell.getColumnName(), timestamp, value});
            }

            run((Function<DSLContext, Void>) ctx -> {
                ctx.insertInto(
                                table(tableName(tableRef)),
                                field(ROW_NAME, byte[].class),
                                field(COL_NAME, byte[].class),
                                field(TIMESTAMP, Long.class),
                                field(VALUE, byte[].class))
                        .select(ctx.select(T1_ROW_NAME, T1_COL_NAME, T1_TIMESTAMP, T1_VALUE)
                                .from(values(ctx, rows, TEMP_TABLE_1, ROW_NAME, COL_NAME, TIMESTAMP, VALUE))
                                .whereNotExists(ctx.selectOne()
                                        .from(atlasTable(tableRef).as(ATLAS_TABLE))
                                        .where(A_ROW_NAME
                                                .eq(T1_ROW_NAME)
                                                .and(A_COL_NAME.eq(T1_COL_NAME))
                                                .and(A_TIMESTAMP.eq(T1_TIMESTAMP)))))
                        .execute();
                return null;
            });
        }
    }

    TableLike<?> values(DSLContext ctx, RowN[] rows, String tableName, String... fieldNames) {
        switch (sqlDialect.family()) {
            case H2:
                List<SelectField<?>> fields = new ArrayList<>(fieldNames.length);
                for (int i = 1; i <= fieldNames.length; i++) {
                    fields.add(DSL.field("C" + i).as(fieldNames[i - 1]));
                }
                RenderContext context = ctx.renderContext();
                context.start(TABLE_VALUES).keyword("values").formatIndentLockStart();

                boolean firstRow = true;
                for (Row row : rows) {
                    if (!firstRow) {
                        context.sql(',').formatSeparator();
                    }

                    context.sql(row.toString());
                    firstRow = false;
                }

                context.formatIndentLockEnd().end(TABLE_VALUES);
                String valuesClause = context.render();
                return ctx.select(fields).from(valuesClause).asTable(tableName);
            default:
                return DSL.values(rows).as(tableName, fieldNames);
        }
    }

    private void putBatch(DSLContext ctx, TableReference tableRef, PutBatch batch, boolean allowReinserts) {
        InsertValuesStep4<Record, byte[], byte[], Long, byte[]> query = ctx.insertInto(
                table(tableName(tableRef)),
                field(ROW_NAME, byte[].class),
                field(COL_NAME, byte[].class),
                field(TIMESTAMP, Long.class),
                field(VALUE, byte[].class));
        query = batch.addValuesForInsert(query);
        try {
            query.execute();
        } catch (DataAccessException e) {
            if (allowReinserts) {
                Result<? extends Record> records = ctx.select(A_ROW_NAME, A_COL_NAME, A_TIMESTAMP, A_VALUE)
                        .from(atlasTable(tableRef).as(ATLAS_TABLE))
                        .where(row(A_ROW_NAME, A_COL_NAME, A_TIMESTAMP).in(batch.getRowsForSelect()))
                        .fetch();
                if (records.isEmpty()) {
                    throw e;
                }
                PutBatch nextBatch = batch.getNextBatch(records);
                if (nextBatch != null) {
                    putBatch(ctx, tableRef, nextBatch, allowReinserts);
                    return;
                }
            }
            throw new KeyAlreadyExistsException("Conflict on table " + tableRef, e);
        }
    }

    @Override
    public void delete(final TableReference tableRef, final Multimap<Cell, Long> keys) {
        if (keys.isEmpty()) {
            return;
        }
        for (List<Map.Entry<Cell, Long>> partition : Iterables.partition(keys.entries(), batchSizeForMutations)) {
            run((Function<DSLContext, Void>) ctx -> {
                Collection<Row3<byte[], byte[], Long>> rows = new ArrayList<>(partition.size());
                for (Map.Entry<Cell, Long> entry : partition) {
                    rows.add(row(entry.getKey().getRowName(), entry.getKey().getColumnName(), entry.getValue()));
                }
                ctx.deleteFrom(atlasTable(tableRef).as(ATLAS_TABLE))
                        .where(row(A_ROW_NAME, A_COL_NAME, A_TIMESTAMP).in(rows))
                        .execute();
                return null;
            });
        }
    }

    @Override
    public void deleteRange(TableReference tableRef, RangeRequest range) {
        try (ClosableIterator<RowResult<Set<Long>>> iterator =
                getRangeOfTimestamps(tableRef, range, AtlasDbConstants.MAX_TS)) {
            while (iterator.hasNext()) {
                RowResult<Set<Long>> rowResult = iterator.next();

                Multimap<Cell, Long> cellsToDelete = HashMultimap.create();
                for (Map.Entry<Cell, Set<Long>> entry : rowResult.getCells()) {
                    cellsToDelete.putAll(entry.getKey(), entry.getValue());
                }

                delete(tableRef, cellsToDelete);
            }
        }
    }

    @Override
    public void deleteRows(TableReference tableRef, Iterable<byte[]> rows) {
        rows.forEach(row -> deleteRange(tableRef, RangeRequests.ofSingleRow(row)));
    }

    @Override
    public void deleteAllTimestamps(TableReference tableRef, Map<Cell, TimestampRangeDelete> deletes) {
        if (deletes.isEmpty()) {
            return;
        }

        long maxTimestampExclusive = deletes.values().stream()
                        .mapToLong(TimestampRangeDelete::maxTimestampToDelete)
                        .max()
                        .getAsLong()
                + 1;

        Multimap<Cell, Long> timestampsByCell = getAllTimestamps(tableRef, deletes.keySet(), maxTimestampExclusive);

        Multimap<Cell, Long> timestampsByCellExcludingSentinels = Multimaps.filterEntries(timestampsByCell, entry -> {
            TimestampRangeDelete delete = deletes.get(entry.getKey());
            long timestamp = entry.getValue();
            return timestamp <= delete.maxTimestampToDelete() && timestamp >= delete.minTimestampToDelete();
        });

        // Sort this to ensure we delete in timestamp ascending order
        SetMultimap<Cell, Long> inSortedOrder = timestampsByCellExcludingSentinels.entries().stream()
                .sorted(Comparator.comparing(Map.Entry::getValue))
                .collect(ImmutableSetMultimap.toImmutableSetMultimap(Map.Entry::getKey, Map.Entry::getValue));

        delete(tableRef, inSortedOrder);
    }

    @Override
    public void truncateTable(TableReference tableRef) throws InsufficientConsistencyException {
        truncateTables(ImmutableSet.of(tableRef));
    }

    @Override
    public void truncateTables(final Set<TableReference> tableRefs) throws InsufficientConsistencyException {
        if (tableRefs.isEmpty()) {
            return;
        }
        run((Function<DSLContext, Void>) ctx -> {
            for (TableReference tableRef : tableRefs) {
                ctx.truncate(tableName(tableRef)).execute();
            }
            return null;
        });
    }

    @Override
    public ClosableIterator<RowResult<Value>> getRange(
            final TableReference tableRef, final RangeRequest rangeRequest, final long timestamp) {
        Iterable<RowResult<Value>> iter =
                new AbstractPagingIterable<RowResult<Value>, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>>() {
                    @Override
                    protected TokenBackedBasicResultsPage<RowResult<Value>, byte[]> getFirstPage() {
                        return getPageWithValues(tableRef, rangeRequest, timestamp);
                    }

                    @Override
                    protected TokenBackedBasicResultsPage<RowResult<Value>, byte[]> getNextPage(
                            TokenBackedBasicResultsPage<RowResult<Value>, byte[]> previous) {
                        byte[] startRow = previous.getTokenForNextPage();
                        RangeRequest newRange = rangeRequest
                                .getBuilder()
                                .startRowInclusive(startRow)
                                .build();
                        return getPageWithValues(tableRef, newRange, timestamp);
                    }
                };
        return ClosableIterators.wrap(iter.iterator());
    }

    @Override
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(
            final TableReference tableRef, final RangeRequest rangeRequest, final long timestamp) {
        Iterable<RowResult<Set<Long>>> iter =
                new AbstractPagingIterable<
                        RowResult<Set<Long>>, TokenBackedBasicResultsPage<RowResult<Set<Long>>, byte[]>>() {
                    @Override
                    protected TokenBackedBasicResultsPage<RowResult<Set<Long>>, byte[]> getFirstPage() {
                        return getPageWithTimestamps(tableRef, rangeRequest, timestamp);
                    }

                    @Override
                    protected TokenBackedBasicResultsPage<RowResult<Set<Long>>, byte[]> getNextPage(
                            TokenBackedBasicResultsPage<RowResult<Set<Long>>, byte[]> previous) {
                        byte[] startRow = previous.getTokenForNextPage();
                        RangeRequest newRange = rangeRequest
                                .getBuilder()
                                .startRowInclusive(startRow)
                                .build();
                        return getPageWithTimestamps(tableRef, newRange, timestamp);
                    }
                };
        return ClosableIterators.wrap(iter.iterator());
    }

    @Override
    public ClosableIterator<List<CandidateCellForSweeping>> getCandidateCellsForSweeping(
            TableReference tableRef, CandidateCellForSweepingRequest request) {
        return new GetCandidateCellsForSweepingShim(this).getCandidateCellsForSweeping(tableRef, request);
    }

    @Override
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(
            TableReference tableRef, Iterable<RangeRequest> rangeRequests, long timestamp) {
        return KeyValueServices.getFirstBatchForRangesUsingGetRange(this, tableRef, rangeRequests, timestamp);
    }

    private TokenBackedBasicResultsPage<RowResult<Value>, byte[]> getPageWithValues(
            final TableReference tableRef, final RangeRequest rangeRequest, final long timestamp) {
        return run((Function<DSLContext, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>>) ctx -> {
            int maxRows = rangeRequest.getBatchHint() == null ? 100 : (int) (1.1 * rangeRequest.getBatchHint());
            Select<Record1<byte[]>> rangeQuery = getRangeQuery(ctx, tableRef, rangeRequest, timestamp, maxRows);
            Select<? extends Record> query;
            if (rangeRequest.getColumnNames().isEmpty()) {
                query = getLatestTimestampQueryAllColumnsSubQuery(ctx, tableRef, rangeQuery, timestamp);
            } else {
                query = getLatestTimestampQuerySomeColumnsSubQuery(
                        ctx, tableRef, rangeQuery, rangeRequest.getColumnNames(), timestamp);
            }
            Result<? extends Record> records = fetchValues(ctx, tableRef, query);
            if (records.isEmpty()) {
                return SimpleTokenBackedResultsPage.create(null, ImmutableList.<RowResult<Value>>of(), false);
            }
            NavigableMap<byte[], SortedMap<byte[], Value>> valuesByRow = breakUpValuesByRow(records);
            if (rangeRequest.isReverse()) {
                valuesByRow = valuesByRow.descendingMap();
            }
            List<RowResult<Value>> finalResults = new ArrayList<>(valuesByRow.size());
            for (Map.Entry<byte[], SortedMap<byte[], Value>> entry : valuesByRow.entrySet()) {
                finalResults.add(RowResult.create(entry.getKey(), entry.getValue()));
            }
            byte[] nextRow = null;
            boolean mayHaveMoreResults = false;
            byte[] lastRow = Iterables.getLast(finalResults).getRowName();
            if (!RangeRequests.isTerminalRow(rangeRequest.isReverse(), lastRow)) {
                nextRow = RangeRequests.getNextStartRow(rangeRequest.isReverse(), lastRow);
                mayHaveMoreResults = finalResults.size() == maxRows;
            }
            return SimpleTokenBackedResultsPage.create(nextRow, finalResults, mayHaveMoreResults);
        });
    }

    private static NavigableMap<byte[], SortedMap<byte[], Value>> breakUpValuesByRow(Result<? extends Record> records) {
        NavigableMap<byte[], SortedMap<byte[], Value>> ret = new TreeMap<>(UnsignedBytes.lexicographicalComparator());
        for (Record record : records) {
            byte[] row = record.getValue(A_ROW_NAME);
            SortedMap<byte[], Value> colMap =
                    ret.computeIfAbsent(row, rowName -> new TreeMap<>(UnsignedBytes.lexicographicalComparator()));
            colMap.put(
                    record.getValue(A_COL_NAME), Value.create(record.getValue(A_VALUE), record.getValue(A_TIMESTAMP)));
        }
        return ret;
    }

    private TokenBackedBasicResultsPage<RowResult<Set<Long>>, byte[]> getPageWithTimestamps(
            final TableReference tableRef, final RangeRequest rangeRequest, final long timestamp) {
        return run((Function<DSLContext, TokenBackedBasicResultsPage<RowResult<Set<Long>>, byte[]>>) ctx -> {
            int maxRows = rangeRequest.getBatchHint() == null ? 100 : (int) (1.1 * rangeRequest.getBatchHint());
            Select<Record1<byte[]>> rangeQuery = getRangeQuery(ctx, tableRef, rangeRequest, timestamp, maxRows);
            Select<? extends Record> query;
            if (rangeRequest.getColumnNames().isEmpty()) {
                query = getAllTimestampsQueryAllColumns(ctx, tableRef, rangeQuery, timestamp);
            } else {
                query = getAllTimestampsQuerySomeColumns(
                        ctx, tableRef, rangeQuery, rangeRequest.getColumnNames(), timestamp);
            }
            Result<? extends Record> records = query.fetch();
            if (records.isEmpty()) {
                return SimpleTokenBackedResultsPage.create(null, ImmutableList.<RowResult<Set<Long>>>of(), false);
            }
            NavigableMap<byte[], SortedMap<byte[], Set<Long>>> timestampsByRow = breakUpTimestampsByRow(records);
            if (rangeRequest.isReverse()) {
                timestampsByRow = timestampsByRow.descendingMap();
            }
            List<RowResult<Set<Long>>> finalResults = new ArrayList<>(timestampsByRow.size());
            for (Map.Entry<byte[], SortedMap<byte[], Set<Long>>> entry : timestampsByRow.entrySet()) {
                finalResults.add(RowResult.create(entry.getKey(), entry.getValue()));
            }
            byte[] nextRow = null;
            boolean mayHaveMoreResults = false;
            byte[] lastRow = Iterables.getLast(finalResults).getRowName();
            if (!RangeRequests.isTerminalRow(rangeRequest.isReverse(), lastRow)) {
                nextRow = RangeRequests.getNextStartRow(rangeRequest.isReverse(), lastRow);
                mayHaveMoreResults = finalResults.size() == maxRows;
            }
            return SimpleTokenBackedResultsPage.create(nextRow, finalResults, mayHaveMoreResults);
        });
    }

    private static NavigableMap<byte[], SortedMap<byte[], Set<Long>>> breakUpTimestampsByRow(
            Result<? extends Record> records) {
        NavigableMap<byte[], SortedMap<byte[], Set<Long>>> ret =
                new TreeMap<>(UnsignedBytes.lexicographicalComparator());
        for (Record record : records) {
            byte[] row = record.getValue(A_ROW_NAME);
            byte[] col = record.getValue(A_COL_NAME);
            SortedMap<byte[], Set<Long>> colMap =
                    ret.computeIfAbsent(row, rowName -> new TreeMap<>(UnsignedBytes.lexicographicalComparator()));
            Set<Long> tsSet = colMap.computeIfAbsent(col, ts -> new HashSet<>());
            tsSet.add(record.getValue(A_TIMESTAMP));
        }
        return ret;
    }

    private SelectOffsetStep<Record1<byte[]>> getRangeQuery(
            DSLContext ctx, TableReference tableRef, RangeRequest rangeRequest, long timestamp, int maxRows) {
        boolean reverse = rangeRequest.isReverse();
        byte[] start = rangeRequest.getStartInclusive();
        byte[] end = rangeRequest.getEndExclusive();
        Condition cond = R_TIMESTAMP.lessThan(timestamp);
        if (start.length > 0) {
            cond = cond.and(reverse ? R_ROW_NAME.lessOrEqual(start) : R_ROW_NAME.greaterOrEqual(start));
        }
        if (end.length > 0) {
            cond = cond.and(reverse ? R_ROW_NAME.greaterThan(end) : R_ROW_NAME.lessThan(end));
        }
        return ctx.selectDistinct(R_ROW_NAME)
                .from(atlasTable(tableRef).as(RANGE_TABLE))
                .where(cond)
                .orderBy(reverse ? R_ROW_NAME.desc() : R_ROW_NAME.asc())
                .limit(maxRows);
    }

    @Override
    public void dropTable(TableReference tableRef) throws InsufficientConsistencyException {
        dropTables(ImmutableSet.of(tableRef));
    }

    @Override
    public void dropTables(final Set<TableReference> tableRefs) throws InsufficientConsistencyException {
        if (tableRefs.isEmpty()) {
            return;
        }
        run((Function<DSLContext, Void>) ctx -> {
            for (TableReference tableRef : tableRefs) {
                ctx.dropTableIfExists(tableName(tableRef)).execute();
            }
            ctx.deleteFrom(METADATA_TABLE).where(TABLE_NAME.in(tableRefs)).execute();
            return null;
        });
    }

    @Override
    public void createTable(TableReference tableRef, byte[] tableMetadata) throws InsufficientConsistencyException {
        createTables(ImmutableMap.of(tableRef, tableMetadata));
    }

    @Override
    public void createTables(final Map<TableReference, byte[]> tableRefToTableMetadata) {
        if (tableRefToTableMetadata.isEmpty()) {
            return;
        }
        run((Function<DSLContext, Void>) ctx -> {
            for (TableReference tableRef : Sets.difference(tableRefToTableMetadata.keySet(), getAllTableNames(ctx))) {
                byte[] metadata = tableRefToTableMetadata.get(tableRef);
                // TODO: Catch and ignore table exists error.
                String partialSql = ctx.createTable(tableName(tableRef))
                        .column(ROW_NAME, VARBINARY.nullable(false))
                        .column(COL_NAME, VARBINARY.nullable(false))
                        .column(TIMESTAMP, BIGINT.nullable(false))
                        .column(VALUE, BLOB)
                        .getSQL();
                int endIndex = partialSql.lastIndexOf(')');
                String fullSql = partialSql.substring(0, endIndex) + "," + " CONSTRAINT "
                        + primaryKey(tableRef) + " PRIMARY KEY ("
                        + ROW_NAME + ", " + COL_NAME + ", " + TIMESTAMP + ")" + partialSql.substring(endIndex);
                try {
                    ctx.execute(fullSql);
                } catch (DataAccessException e) {
                    handleTableCreationException(e);
                }
                ctx.insertInto(METADATA_TABLE, TABLE_NAME, METADATA)
                        .values(tableRef.getQualifiedName(), metadata)
                        .execute();
            }
            return null;
        });
    }

    @Override
    public Set<TableReference> getAllTableNames() {
        return run(this::getAllTableNames);
    }

    private Set<TableReference> getAllTableNames(DSLContext ctx) {
        Result<? extends Record> records =
                ctx.select(TABLE_NAME).from(METADATA_TABLE).fetch();
        Set<TableReference> tableRefs = Sets.newHashSetWithExpectedSize(records.size());
        for (Record record : records) {
            tableRefs.add(TableReference.createUnsafe(record.getValue(TABLE_NAME)));
        }
        return tableRefs;
    }

    @Override
    public byte[] getMetadataForTable(final TableReference tableRef) {
        return run(ctx -> {
            byte[] metadata = ctx.select(METADATA)
                    .from(METADATA_TABLE)
                    .where(TABLE_NAME.eq(tableRef.getQualifiedName()))
                    .fetchOne(METADATA);
            return MoreObjects.firstNonNull(metadata, new byte[0]);
        });
    }

    @Override
    public Map<TableReference, byte[]> getMetadataForTables() {
        return run(ctx -> {
            Result<? extends Record> records =
                    ctx.select(TABLE_NAME, METADATA).from(METADATA_TABLE).fetch();
            Map<TableReference, byte[]> metadata = Maps.newHashMapWithExpectedSize(records.size());
            for (Record record : records) {
                metadata.put(TableReference.createUnsafe(record.getValue(TABLE_NAME)), record.getValue(METADATA));
            }
            return metadata;
        });
    }

    @Override
    public void putMetadataForTable(TableReference tableRef, byte[] metadata) {
        putMetadataForTables(ImmutableMap.of(tableRef, metadata));
    }

    @Override
    public void putMetadataForTables(final Map<TableReference, byte[]> tableRefToMetadata) {
        if (tableRefToMetadata.isEmpty()) {
            return;
        }
        run((Function<DSLContext, Void>) ctx -> {
            Query query =
                    ctx.update(METADATA_TABLE).set(METADATA, (byte[]) null).where(TABLE_NAME.eq((String) null));
            BatchBindStep batch = ctx.batch(query);
            for (Map.Entry<TableReference, byte[]> entry : tableRefToMetadata.entrySet()) {
                batch = batch.bind(entry.getValue(), entry.getKey().getQualifiedName());
            }
            batch.execute();
            return null;
        });
    }

    @Override
    public void compactInternally(final TableReference tableRef) {
        if (sqlDialect.family() == SQLDialect.POSTGRES) {
            run((Function<DSLContext, Void>) ctx -> {
                ctx.execute("VACUUM ANALYZE " + tableName(tableRef));
                return null;
            });
        }
    }

    @Override
    public ClusterAvailabilityStatus getClusterAvailabilityStatus() {
        throw new UnsupportedOperationException("getClusterAvailabilityStatus has not been implemented for Jdbc KVS");
    }

    @Override
    public List<byte[]> getRowKeysInRange(TableReference tableRef, byte[] startRow, byte[] endRow, int maxResults) {
        throw new UnsupportedOperationException("getRowKeysInRange is only supported for Cassandra.");
    }

    @Override
    public ListenableFuture<Map<Cell, Value>> getAsync(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        return Futures.immediateFuture(this.get(tableRef, timestampByCell));
    }

    @Override
    public void close() {
        if (dataSource instanceof Closeable) {
            try {
                ((Closeable) dataSource).close();
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    <T> T run(final Function<DSLContext, T> fun) {
        try (Connection connection = dataSource.getConnection()) {
            DSLContext ctx = DSL.using(connection, sqlDialect, settings);
            return fun.apply(ctx);
        } catch (SQLException e) {
            throw new DataAccessException("Error handling connection from data source " + dataSource, e);
        }
    }

    <T> T runInTransaction(final Function<DSLContext, T> fun) {
        try (Connection connection = dataSource.getConnection()) {
            final DSLContext ctx = DSL.using(connection, sqlDialect, settings);
            return ctx.transactionResult(configuration -> fun.apply(ctx));
        } catch (SQLException e) {
            throw new DataAccessException("Error handling connection from data source " + dataSource, e);
        }
    }

    void handleTableCreationException(DataAccessException e) {
        if (e.getMessage().contains("already exists")) {
            return;
        }
        if (e.getMessage().contains("already used")) {
            return;
        }
        throw e;
    }

    /**
     * Take a given table name of arbitrary length and reduce it to a fixed
     * length (30 characters) for portability across rdbms impls that have
     * maximum lengths for table names.
     * <p>
     * Make a simple attempt to keep the name human readable by taking a
     * portion from the beginning, a portion from the end, and appending
     * a short hash.
     */
    String tableName(TableReference tableRef) {
        String rawName = tableRef.getQualifiedName();
        String hash = hashTableName(rawName).substring(0, 8);
        if (tablePrefix.length() + rawName.length() + hash.length() < 30) {
            return tablePrefix + rawName.replace('.', '_') + '_' + hash;
        } else {
            String fullName = tablePrefix + rawName.replace('.', '_');
            return fullName.substring(0, 10) + '_' + fullName.substring(fullName.length() - 10) + '_' + hash;
        }
    }

    Table<Record> atlasTable(TableReference tableRef) {
        return table(tableName(tableRef));
    }

    String primaryKey(TableReference tableRef) {
        String hash = hashTableName(tableRef.getQualifiedName()).substring(0, 16);
        return "pk_" + tablePrefix + hash;
    }

    String hashTableName(String rawName) {
        // Take 40 bits from the raw name
        byte[] bytes = Hashing.murmur3_128()
                .hashString(rawName, StandardCharsets.UTF_8)
                .asBytes();
        return BaseEncoding.base32().omitPadding().encode(bytes);
    }

    @Override
    public Map<byte[], RowColumnRangeIterator> getRowsColumnRange(
            TableReference tableRef,
            Iterable<byte[]> rows,
            BatchColumnRangeSelection batchColumnRangeSelection,
            long timestamp) {
        return KeyValueServices.filterGetRowsToColumnRange(this, tableRef, rows, batchColumnRangeSelection, timestamp);
    }

    @Override
    public RowColumnRangeIterator getRowsColumnRange(
            TableReference tableRef,
            Iterable<byte[]> rows,
            ColumnRangeSelection columnRangeSelection,
            int cellBatchHint,
            long timestamp) {
        return KeyValueServices.mergeGetRowsColumnRangeIntoSingleIterator(
                this, tableRef, rows, columnRangeSelection, cellBatchHint, timestamp);
    }
}
