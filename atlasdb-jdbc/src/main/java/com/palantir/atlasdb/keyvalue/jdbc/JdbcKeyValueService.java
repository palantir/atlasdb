/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.jdbc;

import static org.jooq.Clause.TABLE_VALUES;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.row;
import static org.jooq.impl.DSL.table;
import static org.jooq.impl.SQLDataType.BIGINT;
import static org.jooq.impl.SQLDataType.BLOB;
import static org.jooq.impl.SQLDataType.VARBINARY;
import static org.jooq.impl.SQLDataType.VARCHAR;

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

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;

import javax.sql.DataSource;

import org.jooq.BatchBindStep;
import org.jooq.Condition;
import org.jooq.Configuration;
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
import org.jooq.TransactionalCallable;
import org.jooq.conf.RenderNameStyle;
import org.jooq.conf.Settings;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.base.Throwables;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.jdbc.config.JdbcDataSourceConfiguration;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.KeyValueServices;
import com.palantir.atlasdb.keyvalue.jdbc.impl.MultiTimestampPutBatch;
import com.palantir.atlasdb.keyvalue.jdbc.impl.PutBatch;
import com.palantir.atlasdb.keyvalue.jdbc.impl.SingleTimestampPutBatch;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.util.paging.AbstractPagingIterable;
import com.palantir.util.paging.SimpleTokenBackedResultsPage;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

public class JdbcKeyValueService implements KeyValueService {
    private final String tablePrefix;
    private final SQLDialect sqlDialect;
    private final DataSource dataSource;
    private final Settings settings;

    public final Table<Record> METADATA_TABLE;

    private JdbcKeyValueService(String tablePrefix,
                                SQLDialect sqlDialect,
                                DataSource dataSource,
                                Settings settings) {
        this.tablePrefix = tablePrefix;
        this.sqlDialect = sqlDialect;
        this.dataSource = dataSource;
        this.settings = settings;

        METADATA_TABLE = table(tablePrefix + "_metadata");
    }

    public static JdbcKeyValueService create(JdbcKeyValueConfiguration config) {
        JdbcDataSourceConfiguration dataSourceConfig = config.getDataSourceConfig();
        SQLDialect sqlDialect = SQLDialect.valueOf(dataSourceConfig.getSqlDialect());
        DataSource dataSource = dataSourceConfig.createDataSource();
        Settings settings = new Settings();
        settings.setRenderNameStyle(RenderNameStyle.AS_IS);
        final JdbcKeyValueService kvs = new JdbcKeyValueService(config.getTablePrefix(), sqlDialect, dataSource, settings);

        kvs.run(new Function<DSLContext, Void>() {
            @Override
            public Void apply(DSLContext ctx) {
                String partialSql = ctx.createTable(kvs.METADATA_TABLE)
                        .column(TABLE_NAME, VARCHAR.nullable(false))
                        .column(METADATA, BLOB.nullable(false))
                        .getSQL();
                int endIndex = partialSql.lastIndexOf(')');
                String fullSql = partialSql.substring(0, endIndex) + "," +
                        " CONSTRAINT pk_" + kvs.METADATA_TABLE.getName() +
                        " PRIMARY KEY (" + TABLE_NAME.getName() + ")" +
                        partialSql.substring(endIndex);
                try {
                    ctx.execute(fullSql);
                } catch (DataAccessException e) {
                    kvs.handleTableCreationException(e);
                }
                return null;
            }
        });

        return kvs;
    }

    @Override
    public void initializeFromFreshInstance() {
        // do nothing
    }

    @Override
    public Collection<? extends KeyValueService> getDelegates() {
        return ImmutableList.of();
    }

    @Override
    public Map<Cell, Value> getRows(String tableName,
                                    Iterable<byte[]> rows,
                                    ColumnSelection columnSelection,
                                    long timestamp) {
        if (columnSelection.allColumnsSelected()) {
            return getRowsAllColumns(tableName, rows, timestamp);
        } else {
            return getRowsSomeColumns(tableName, rows, columnSelection, timestamp);
        }
    }

    private Map<Cell, Value> getRowsAllColumns(final String tableName,
                                               final Iterable<byte[]> rows,
                                               final long timestamp) {
        if (Iterables.isEmpty(rows)) {
            return ImmutableMap.of();
        }
        return run(new Function<DSLContext, Map<Cell, Value>>() {
            @Override
            public Map<Cell, Value> apply(DSLContext ctx) {
                Select<? extends Record> query = getLatestTimestampQueryAllColumns(
                        ctx,
                        tableName,
                        ImmutableList.copyOf(rows),
                        timestamp);
                Result<? extends Record> records = fetchValues(ctx, tableName, query);
                Map<Cell, Value> results = Maps.newHashMapWithExpectedSize(records.size());
                for (Record record : records) {
                    results.put(
                            Cell.create(record.getValue(A_ROW_NAME), record.getValue(A_COL_NAME)),
                            Value.create(record.getValue(A_VALUE), record.getValue(A_TIMESTAMP)));
                }
                return results;
            }
        });
    }

    private Map<Cell, Value> getRowsSomeColumns(final String tableName,
                                                final Iterable<byte[]> rows,
                                                final ColumnSelection columnSelection,
                                                final long timestamp) {
        if (Iterables.isEmpty(rows)) {
            return ImmutableMap.of();
        }
        return run(new Function<DSLContext, Map<Cell, Value>>() {
            @Override
            public Map<Cell, Value> apply(DSLContext ctx) {
                Select<? extends Record> query = getLatestTimestampQuerySomeColumns(
                        ctx,
                        tableName,
                        ImmutableList.copyOf(rows),
                        columnSelection.getSelectedColumns(),
                        timestamp);
                Result<? extends Record> records = fetchValues(ctx, tableName, query);
                Map<Cell, Value> results = Maps.newHashMapWithExpectedSize(records.size());
                for (Record record : records) {
                    results.put(
                            Cell.create(record.getValue(A_ROW_NAME), record.getValue(A_COL_NAME)),
                            Value.create(record.getValue(A_VALUE), record.getValue(A_TIMESTAMP)));
                }
                return results;
            }
        });
    }

    @Override
    public Map<Cell, Value> get(final String tableName,
                                final Map<Cell, Long> timestampByCell) {
        if (timestampByCell.isEmpty()) {
            return ImmutableMap.of();
        }
        return run(new Function<DSLContext, Map<Cell, Value>>() {
            @Override
            public Map<Cell, Value> apply(DSLContext ctx) {
                Select<? extends Record> query = getLatestTimestampQueryManyTimestamps(
                        ctx,
                        tableName,
                        toRows(timestampByCell));
                Result<? extends Record> records = fetchValues(ctx, tableName, query);
                Map<Cell, Value> results = Maps.newHashMapWithExpectedSize(records.size());
                for (Record record : records) {
                    results.put(
                            Cell.create(record.getValue(A_ROW_NAME), record.getValue(A_COL_NAME)),
                            Value.create(record.getValue(A_VALUE), record.getValue(A_TIMESTAMP)));
                }
                return results;
            }
        });
    }

    @Override
    public Map<Cell, Long> getLatestTimestamps(final String tableName,
                                               final Map<Cell, Long> timestampByCell) {
        if (timestampByCell.isEmpty()) {
            return ImmutableMap.of();
        }
        return run(new Function<DSLContext, Map<Cell, Long>>() {
            @Override
            public Map<Cell, Long> apply(DSLContext ctx) {
                Select<? extends Record> query = getLatestTimestampQueryManyTimestamps(
                        ctx,
                        tableName,
                        toRows(timestampByCell));
                Result<? extends Record> records = query.fetch();
                Map<Cell, Long> results = Maps.newHashMapWithExpectedSize(records.size());
                for (Record record : records) {
                    results.put(
                            Cell.create(record.getValue(A_ROW_NAME), record.getValue(A_COL_NAME)),
                            record.getValue(MAX_TIMESTAMP, Long.class));
                }
                return results;
            }
        });
    }

    @Override
    public Multimap<Cell, Long> getAllTimestamps(final String tableName,
                                                 final Set<Cell> cells,
                                                 final long timestamp) throws InsufficientConsistencyException {
        if (cells.isEmpty()) {
            return ImmutableMultimap.of();
        }
        return run(new Function<DSLContext, Multimap<Cell, Long>>() {
            @Override
            public Multimap<Cell, Long> apply(DSLContext ctx) {
                Result<? extends Record> records = ctx
                        .select(A_ROW_NAME, A_COL_NAME, A_TIMESTAMP)
                        .from(atlasTable(tableName).as(ATLAS_TABLE))
                        .join(values(ctx, toRows(cells), TEMP_TABLE_1, ROW_NAME, COL_NAME))
                        .on(A_ROW_NAME.eq(T1_ROW_NAME)
                                .and(A_COL_NAME.eq(T1_COL_NAME)))
                        .where(A_TIMESTAMP.lessThan(timestamp))
                        .fetch();
                Multimap<Cell, Long> results = ArrayListMultimap.create(records.size() / 4, 4);
                for (Record record : records) {
                    results.put(
                            Cell.create(record.getValue(A_ROW_NAME), record.getValue(A_COL_NAME)),
                            record.getValue(A_TIMESTAMP));
                }
                return results;
            }
        });
    }

    private static RowN[] toRows(Set<Cell> cells) {
        RowN[] rows = new RowN[cells.size()];
        int i = 0;
        for (Cell cell : cells) {
            rows[i++] = row(new Object[] {cell.getRowName(), cell.getColumnName()});
        }
        return rows;
    }

    private static RowN[] toRows(Map<Cell, Long> timestampByCell) {
        RowN[] rows = new RowN[timestampByCell.size()];
        int i = 0;
        for (Entry<Cell, Long> entry : timestampByCell.entrySet()) {
            rows[i++] = row(new Object[] {entry.getKey().getRowName(), entry.getKey().getColumnName(), entry.getValue()});
        }
        return rows;
    }

    private Select<? extends Record> getLatestTimestampQueryAllColumns(DSLContext ctx,
                                                                       String tableName,
                                                                       Collection<byte[]> rows,
                                                                       long timestamp) {
        return ctx.select(A_ROW_NAME, A_COL_NAME, DSL.max(A_TIMESTAMP).as(MAX_TIMESTAMP))
                .from(atlasTable(tableName).as(ATLAS_TABLE))
                .where(A_ROW_NAME.in(rows)
                        .and(A_TIMESTAMP.lessThan(timestamp)))
                .groupBy(A_ROW_NAME, A_COL_NAME);
    }

    private Select<? extends Record> getLatestTimestampQueryAllColumnsSubQuery(DSLContext ctx,
                                                                               String tableName,
                                                                               Select<Record1<byte[]>> subQuery,
                                                                               long timestamp) {
        return ctx.select(A_ROW_NAME, A_COL_NAME, DSL.max(A_TIMESTAMP).as(MAX_TIMESTAMP))
                .from(atlasTable(tableName).as(ATLAS_TABLE))
                .where(A_ROW_NAME.in(subQuery)
                        .and(A_TIMESTAMP.lessThan(timestamp)))
                .groupBy(A_ROW_NAME, A_COL_NAME);
    }

    private Select<? extends Record> getLatestTimestampQuerySomeColumnsSubQuery(DSLContext ctx,
                                                                                String tableName,
                                                                                Select<Record1<byte[]>> subQuery,
                                                                                Collection<byte[]> cols,
                                                                                long timestamp) {
        return ctx.select(A_ROW_NAME, A_COL_NAME, DSL.max(A_TIMESTAMP).as(MAX_TIMESTAMP))
                .from(atlasTable(tableName).as(ATLAS_TABLE))
                .where(A_ROW_NAME.in(subQuery)
                        .and(A_COL_NAME.in(cols)))
                        .and(A_TIMESTAMP.lessThan(timestamp))
                .groupBy(A_ROW_NAME, A_COL_NAME);
    }

    private Select<? extends Record> getLatestTimestampQuerySomeColumns(DSLContext ctx,
                                                                        String tableName,
                                                                        Collection<byte[]> rows,
                                                                        Collection<byte[]> cols,
                                                                        long timestamp) {
        return ctx.select(A_ROW_NAME, A_COL_NAME, DSL.max(A_TIMESTAMP).as(MAX_TIMESTAMP))
                .from(atlasTable(tableName).as(ATLAS_TABLE))
                .where(A_ROW_NAME.in(rows)
                        .and(A_COL_NAME.in(cols)))
                        .and(A_TIMESTAMP.lessThan(timestamp))
                .groupBy(A_ROW_NAME, A_COL_NAME);
    }

    private Select<? extends Record> getAllTimestampsQueryAllColumns(DSLContext ctx,
                                                                     String tableName,
                                                                     Select<Record1<byte[]>> subQuery,
                                                                     long timestamp) {
        return ctx.select(A_ROW_NAME, A_COL_NAME, A_TIMESTAMP)
                .from(atlasTable(tableName).as(ATLAS_TABLE))
                .where(A_ROW_NAME.in(subQuery)
                        .and(A_TIMESTAMP.lessThan(timestamp)));
    }

    private Select<? extends Record> getAllTimestampsQuerySomeColumns(DSLContext ctx,
                                                                      String tableName,
                                                                      Select<Record1<byte[]>> subQuery,
                                                                      Collection<byte[]> cols,
                                                                      long timestamp) {
        return ctx.select(A_ROW_NAME, A_COL_NAME, A_TIMESTAMP)
                .from(atlasTable(tableName).as(ATLAS_TABLE))
                .where(A_ROW_NAME.in(subQuery)
                        .and(A_COL_NAME.in(cols)))
                        .and(A_TIMESTAMP.lessThan(timestamp));
    }

    private Select<? extends Record> getLatestTimestampQueryManyTimestamps(DSLContext ctx,
                                                                           String tableName,
                                                                           RowN[] rows) {
        return ctx.select(A_ROW_NAME, A_COL_NAME, DSL.max(A_TIMESTAMP).as(MAX_TIMESTAMP))
                .from(atlasTable(tableName).as(ATLAS_TABLE))
                .join(values(ctx, rows, TEMP_TABLE_1, ROW_NAME, COL_NAME, TIMESTAMP))
                .on(A_ROW_NAME.eq(T1_ROW_NAME)
                        .and(A_COL_NAME.eq(T1_COL_NAME)))
                .where(A_TIMESTAMP.lessThan(T1_TIMESTAMP))
                .groupBy(A_ROW_NAME, A_COL_NAME);
    }

    private Result<? extends Record> fetchValues(DSLContext ctx,
                                                 String tableName,
                                                 Select<? extends Record> subQuery) {
        return ctx.select(A_ROW_NAME, A_COL_NAME, A_TIMESTAMP, A_VALUE)
                .from(atlasTable(tableName).as(ATLAS_TABLE))
                .join(subQuery.asTable(TEMP_TABLE_2))
                .on(A_ROW_NAME.eq(T2_ROW_NAME)
                        .and(A_COL_NAME.eq(T2_COL_NAME))
                        .and(A_TIMESTAMP.eq(T2_MAX_TIMESTAMP)))
                .fetch();
    }

    @Override
    public void put(final String tableName,
                    final Map<Cell, byte[]> values,
                    final long timestamp) throws KeyAlreadyExistsException {
        if (values.isEmpty()) {
            return;
        }
        run(new Function<DSLContext, Void>() {
            @Override
            public Void apply(DSLContext ctx) {
                putBatch(ctx, tableName, new SingleTimestampPutBatch(values, timestamp), true);
                return null;
            }
        });
    }

    @Override
    public void multiPut(final Map<String, ? extends Map<Cell, byte[]>> valuesByTable,
                         final long timestamp) throws KeyAlreadyExistsException {
        run(new Function<DSLContext, Void>() {
            @Override
            public Void apply(DSLContext ctx) {
                for (Entry<String, ? extends Map<Cell, byte[]>> entry : valuesByTable.entrySet()) {
                    String tableName = entry.getKey();
                    Map<Cell, byte[]> values = entry.getValue();
                    if (!values.isEmpty()) {
                        putBatch(ctx, tableName, new SingleTimestampPutBatch(values, timestamp), true);
                    }
                }
                return null;
            }
        });
    }

    @Override
    public void putWithTimestamps(final String tableName,
                                  final Multimap<Cell, Value> values) throws KeyAlreadyExistsException {
        if (values.isEmpty()) {
            return;
        }
        run(new Function<DSLContext, Void>() {
            @Override
            public Void apply(DSLContext ctx) {
                putBatch(ctx, tableName, new MultiTimestampPutBatch(values), true);
                return null;
            }
        });
    }

    @Override
    public void putUnlessExists(final String tableName,
                                final Map<Cell, byte[]> values) throws KeyAlreadyExistsException {
        if (values.isEmpty()) {
            return;
        }
        run(new Function<DSLContext, Void>() {
            @Override
            public Void apply(DSLContext ctx) {
                putBatch(ctx, tableName, new SingleTimestampPutBatch(values, 0L), false);
                return null;
            }
        });
    }

    @Override
    public void addGarbageCollectionSentinelValues(final String tableName, Set<Cell> cells) {
        if (cells.isEmpty()) {
            return;
        }
        Long timestamp = Value.INVALID_VALUE_TIMESTAMP;
        byte[] value = new byte[0];
        final RowN[] rows = new RowN[cells.size()];
        int i = 0;
        for (Cell cell : cells) {
            rows[i++] = row(new Object[] {cell.getRowName(), cell.getColumnName(), timestamp, value});
        }
        run(new Function<DSLContext, Void>() {
            @Override
            public Void apply(DSLContext ctx) {
                ctx.insertInto(table(tableName(tableName)),
                        field(ROW_NAME, byte[].class),
                        field(COL_NAME, byte[].class),
                        field(TIMESTAMP, Long.class),
                        field(VALUE, byte[].class))
                .select(ctx.select(T1_ROW_NAME, T1_COL_NAME, T1_TIMESTAMP, T1_VALUE)
                        .from(values(ctx, rows, TEMP_TABLE_1, ROW_NAME, COL_NAME, TIMESTAMP, VALUE))
                        .whereNotExists(ctx.selectOne()
                                .from(atlasTable(tableName).as(ATLAS_TABLE))
                                .where(A_ROW_NAME.eq(T1_ROW_NAME)
                                        .and(A_COL_NAME.eq(T1_COL_NAME))
                                        .and(A_TIMESTAMP.eq(T1_TIMESTAMP)))))
                .execute();
                return null;
            }
        });
    }

    TableLike<?> values(DSLContext ctx, RowN[] rows, String tableName, String... fieldNames) {
        switch (sqlDialect.family()) {
        case H2:
            List<SelectField<?>> fields = Lists.newArrayListWithCapacity(fieldNames.length);
            for (int i = 1; i <= fieldNames.length; i++) {
                fields.add(DSL.field("C" + i).as(fieldNames[i-1]));
            }
            RenderContext context = ctx.renderContext();
            context.start(TABLE_VALUES)
                .keyword("values")
                .formatIndentLockStart();

            boolean firstRow = true;
            for (Row row : rows) {
                if (!firstRow) {
                    context.sql(',').formatSeparator();
                }

                context.sql(row.toString());
                firstRow = false;
            }

            context.formatIndentLockEnd()
                .end(TABLE_VALUES);
            String valuesClause = context.render();
            return ctx.select(fields).from(valuesClause).asTable(tableName);
        default:
            return DSL.values(rows).as(tableName, fieldNames);
        }
    }

    private void putBatch(DSLContext ctx, String tableName, PutBatch batch, boolean allowReinserts) {
        InsertValuesStep4<Record, byte[], byte[], Long, byte[]> query =
                ctx.insertInto(table(tableName(tableName)),
                        field(ROW_NAME, byte[].class),
                        field(COL_NAME, byte[].class),
                        field(TIMESTAMP, Long.class),
                        field(VALUE, byte[].class));
        query = batch.addValuesForInsert(query);
        try {
            query.execute();
        } catch (DataAccessException e) {
            if (allowReinserts) {
                Result<? extends Record> records = ctx
                        .select(A_ROW_NAME, A_COL_NAME, A_TIMESTAMP, A_VALUE)
                        .from(atlasTable(tableName).as(ATLAS_TABLE))
                        .where(row(A_ROW_NAME, A_COL_NAME, A_TIMESTAMP).in(batch.getRowsForSelect()))
                        .fetch();
                if (records.isEmpty()) {
                    throw e;
                }
                PutBatch nextBatch = batch.getNextBatch(records);
                if (nextBatch != null) {
                    putBatch(ctx, tableName, nextBatch, allowReinserts);
                    return;
                }
            }
            throw new KeyAlreadyExistsException("Conflict on table " + tableName, e);
        }
    }

    @Override
    public void delete(final String tableName, final Multimap<Cell, Long> keys) {
        if (keys.isEmpty()) {
            return;
        }
        run(new Function<DSLContext, Void>() {
            @Override
            public Void apply(DSLContext ctx) {
                Collection<Row3<byte[], byte[], Long>> rows = Lists.newArrayListWithCapacity(keys.size());
                for (Entry<Cell, Long> entry : keys.entries()) {
                    rows.add(row(entry.getKey().getRowName(), entry.getKey().getColumnName(), entry.getValue()));
                }
                ctx.deleteFrom(atlasTable(tableName).as(ATLAS_TABLE))
                    .where(row(A_ROW_NAME, A_COL_NAME, A_TIMESTAMP).in(rows))
                    .execute();
                return null;
            }
        });
    }

    @Override
    public void truncateTable(String tableName) throws InsufficientConsistencyException {
        truncateTables(ImmutableSet.of(tableName));
    }

    @Override
    public void truncateTables(final Set<String> tableNames) throws InsufficientConsistencyException {
        if (tableNames.isEmpty()) {
            return;
        }
        run(new Function<DSLContext, Void>() {
            @Override
            public Void apply(DSLContext ctx) {
                for (String tableName : tableNames) {
                    ctx.truncate(tableName(tableName)).execute();
                }
                return null;
            }
        });
    }

    @Override
    public ClosableIterator<RowResult<Value>> getRange(final String tableName,
                                                       final RangeRequest rangeRequest,
                                                       final long timestamp) {
        Iterable<RowResult<Value>> iter = new AbstractPagingIterable<RowResult<Value>, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>>() {
            @Override
            protected TokenBackedBasicResultsPage<RowResult<Value>, byte[]> getFirstPage() {
                return getPageWithValues(tableName, rangeRequest, timestamp);
            }

            @Override
            protected TokenBackedBasicResultsPage<RowResult<Value>, byte[]> getNextPage(TokenBackedBasicResultsPage<RowResult<Value>, byte[]> previous) {
                byte[] startRow = previous.getTokenForNextPage();
                RangeRequest newRange = rangeRequest.getBuilder().startRowInclusive(startRow).build();
                return getPageWithValues(tableName, newRange, timestamp);
            }
        };
        return ClosableIterators.wrap(iter.iterator());
    }

    @Override
    public ClosableIterator<RowResult<Set<Value>>> getRangeWithHistory(String tableName,
                                                                       RangeRequest rangeRequest,
                                                                       long timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(final String tableName,
                                                                       final RangeRequest rangeRequest,
                                                                       final long timestamp) {
        Iterable<RowResult<Set<Long>>> iter = new AbstractPagingIterable<RowResult<Set<Long>>, TokenBackedBasicResultsPage<RowResult<Set<Long>>, byte[]>>() {
            @Override
            protected TokenBackedBasicResultsPage<RowResult<Set<Long>>, byte[]> getFirstPage() {
                return getPageWithTimestamps(tableName, rangeRequest, timestamp);
            }

            @Override
            protected TokenBackedBasicResultsPage<RowResult<Set<Long>>, byte[]> getNextPage(TokenBackedBasicResultsPage<RowResult<Set<Long>>, byte[]> previous) {
                byte[] startRow = previous.getTokenForNextPage();
                RangeRequest newRange = rangeRequest.getBuilder().startRowInclusive(startRow).build();
                return getPageWithTimestamps(tableName, newRange, timestamp);
            }
        };
        return ClosableIterators.wrap(iter.iterator());
    }

    @Override
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(String tableName,
                                                                                                           Iterable<RangeRequest> rangeRequests,
                                                                                                           long timestamp) {
        return KeyValueServices.getFirstBatchForRangesUsingGetRange(this, tableName, rangeRequests, timestamp);
    }

    private TokenBackedBasicResultsPage<RowResult<Value>, byte[]> getPageWithValues(final String tableName,
                                                                                    final RangeRequest rangeRequest,
                                                                                    final long timestamp) {
        return run(new Function<DSLContext, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>>() {
            @Override
            public TokenBackedBasicResultsPage<RowResult<Value>, byte[]> apply(DSLContext ctx) {
                int maxRows = rangeRequest.getBatchHint() == null ? 100 : (int) (1.1 * rangeRequest.getBatchHint());
                Select<Record1<byte[]>> rangeQuery = getRangeQuery(ctx, tableName, rangeRequest, timestamp, maxRows);
                Select<? extends Record> query;
                if (rangeRequest.getColumnNames().isEmpty()) {
                    query = getLatestTimestampQueryAllColumnsSubQuery(ctx, tableName, rangeQuery, timestamp);
                } else {
                    query = getLatestTimestampQuerySomeColumnsSubQuery(ctx, tableName, rangeQuery, rangeRequest.getColumnNames(), timestamp);
                }
                Result<? extends Record> records = fetchValues(ctx, tableName, query);
                if (records.isEmpty()) {
                    return SimpleTokenBackedResultsPage.create(null, ImmutableList.<RowResult<Value>>of(), false);
                }
                NavigableMap<byte[], SortedMap<byte[], Value>> valuesByRow = breakUpValuesByRow(records);
                if (rangeRequest.isReverse()) {
                    valuesByRow = valuesByRow.descendingMap();
                }
                List<RowResult<Value>> finalResults = Lists.newArrayListWithCapacity(valuesByRow.size());
                for (Entry<byte[], SortedMap<byte[], Value>> entry : valuesByRow.entrySet()) {
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
            }
        });
    }

    private static NavigableMap<byte[], SortedMap<byte[], Value>> breakUpValuesByRow(Result<? extends Record> records) {
        NavigableMap<byte[], SortedMap<byte[], Value>> ret = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());
        for (Record record : records) {
            byte[] row = record.getValue(A_ROW_NAME);
            SortedMap<byte[], Value> colMap = ret.get(row);
            if (colMap == null) {
                colMap = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());
                ret.put(row, colMap);
            }
            colMap.put(record.getValue(A_COL_NAME), Value.create(record.getValue(A_VALUE), record.getValue(A_TIMESTAMP)));
        }
        return ret;
    }

    private TokenBackedBasicResultsPage<RowResult<Set<Long>>, byte[]> getPageWithTimestamps(final String tableName,
                                                                                            final RangeRequest rangeRequest,
                                                                                            final long timestamp) {
        return run(new Function<DSLContext, TokenBackedBasicResultsPage<RowResult<Set<Long>>, byte[]>>() {
            @Override
            public TokenBackedBasicResultsPage<RowResult<Set<Long>>, byte[]> apply(DSLContext ctx) {
                int maxRows = rangeRequest.getBatchHint() == null ? 100 : (int) (1.1 * rangeRequest.getBatchHint());
                Select<Record1<byte[]>> rangeQuery = getRangeQuery(ctx, tableName, rangeRequest, timestamp, maxRows);
                Select<? extends Record> query;
                if (rangeRequest.getColumnNames().isEmpty()) {
                    query = getAllTimestampsQueryAllColumns(ctx, tableName, rangeQuery, timestamp);
                } else {
                    query = getAllTimestampsQuerySomeColumns(ctx, tableName, rangeQuery, rangeRequest.getColumnNames(), timestamp);
                }
                Result<? extends Record> records = query.fetch();
                if (records.isEmpty()) {
                    return SimpleTokenBackedResultsPage.create(null, ImmutableList.<RowResult<Set<Long>>>of(), false);
                }
                NavigableMap<byte[], SortedMap<byte[], Set<Long>>> timestampsByRow = breakUpTimestampsByRow(records);
                if (rangeRequest.isReverse()) {
                    timestampsByRow = timestampsByRow.descendingMap();
                }
                List<RowResult<Set<Long>>> finalResults = Lists.newArrayListWithCapacity(timestampsByRow.size());
                for (Entry<byte[], SortedMap<byte[], Set<Long>>> entry : timestampsByRow.entrySet()) {
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
            }
        });
    }

    private static NavigableMap<byte[], SortedMap<byte[], Set<Long>>> breakUpTimestampsByRow(Result<? extends Record> records) {
        NavigableMap<byte[], SortedMap<byte[], Set<Long>>> ret = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());
        for (Record record : records) {
            byte[] row = record.getValue(A_ROW_NAME);
            byte[] col = record.getValue(A_COL_NAME);
            SortedMap<byte[], Set<Long>> colMap = ret.get(row);
            if (colMap == null) {
                colMap = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());
                ret.put(row, colMap);
            }
            Set<Long> tsSet = colMap.get(col);
            if (tsSet == null) {
                tsSet = Sets.newHashSet();
                colMap.put(col, tsSet);
            }
            tsSet.add(record.getValue(A_TIMESTAMP));
        }
        return ret;
    }

    private SelectOffsetStep<Record1<byte[]>> getRangeQuery(DSLContext ctx,
                                                            String tableName,
                                                            RangeRequest rangeRequest,
                                                            long timestamp,
                                                            int maxRows) {
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
                .from(atlasTable(tableName).as(RANGE_TABLE))
                .where(cond)
                .orderBy(reverse ? R_ROW_NAME.desc() : R_ROW_NAME.asc())
                .limit(maxRows);
    }

    @Override
    public void dropTable(String tableName) throws InsufficientConsistencyException {
        dropTables(ImmutableSet.of(tableName));
    }

    @Override
    public void dropTables(final Set<String> tableNames) throws InsufficientConsistencyException {
        if (tableNames.isEmpty()) {
            return;
        }
        run(new Function<DSLContext, Void>() {
            @Override
            public Void apply(DSLContext ctx) {
                for (String tableName : tableNames) {
                    ctx.dropTableIfExists(tableName(tableName)).execute();
                }
                ctx.deleteFrom(METADATA_TABLE)
                    .where(TABLE_NAME.in(tableNames))
                    .execute();
                return null;
            }
        });
    }

    @Override
    public void createTable(String tableName, byte[] tableMetadata)
            throws InsufficientConsistencyException {
        createTables(ImmutableMap.of(tableName, tableMetadata));
    }

    @Override
    public void createTables(final Map<String, byte[]> tableNameToTableMetadata) {
        if (tableNameToTableMetadata.isEmpty()) {
            return;
        }
        run(new Function<DSLContext, Void>() {
            @Override
            public Void apply(DSLContext ctx) {
                for (String tableName : Sets.difference(tableNameToTableMetadata.keySet(), getAllTableNames(ctx))) {
                    byte[] metadata = tableNameToTableMetadata.get(tableName);
                    // TODO: Catch and ignore table exists error.
                    String partialSql = ctx.createTable(tableName(tableName))
                        .column(ROW_NAME, VARBINARY.nullable(false))
                        .column(COL_NAME, VARBINARY.nullable(false))
                        .column(TIMESTAMP, BIGINT.nullable(false))
                        .column(VALUE, BLOB)
                        .getSQL();
                    int endIndex = partialSql.lastIndexOf(')');
                    String fullSql = partialSql.substring(0, endIndex) + "," +
                            " CONSTRAINT " + primaryKey(tableName) +
                            " PRIMARY KEY (" + ROW_NAME + ", " + COL_NAME + ", " + TIMESTAMP + ")" +
                            partialSql.substring(endIndex);
                    try {
                        ctx.execute(fullSql);
                    } catch (DataAccessException e) {
                        handleTableCreationException(e);
                    }
                    ctx.insertInto(METADATA_TABLE, TABLE_NAME, METADATA)
                        .values(tableName, metadata)
                        .execute();
                }
                return null;
            }
        });
    }

    @Override
    public Set<String> getAllTableNames() {
        return run(new Function<DSLContext, Set<String>>() {
            @Override
            public Set<String> apply(DSLContext ctx) {
                return getAllTableNames(ctx);
            }
        });
    }

    private Set<String> getAllTableNames(DSLContext ctx) {
        Result<? extends Record> records = ctx
                .select(TABLE_NAME)
                .from(METADATA_TABLE)
                .fetch();
        Set<String> tableNames = Sets.newHashSetWithExpectedSize(records.size());
        for (Record record : records) {
            tableNames.add(record.getValue(TABLE_NAME));
        }
        return tableNames;
    }

    @Override
    public byte[] getMetadataForTable(final String tableName) {
        return run(new Function<DSLContext, byte[]>() {
            @Override
            public byte[] apply(DSLContext ctx) {
                byte[] metadata = ctx
                        .select(METADATA)
                        .from(METADATA_TABLE)
                        .where(TABLE_NAME.eq(tableName))
                        .fetchOne(METADATA);
                return MoreObjects.firstNonNull(metadata, new byte[0]);
            }
        });
    }

    @Override
    public Map<String, byte[]> getMetadataForTables() {
        return run(new Function<DSLContext, Map<String, byte[]>>() {
            @Override
            public Map<String, byte[]> apply(DSLContext ctx) {
                Result<? extends Record> records = ctx
                        .select(TABLE_NAME, METADATA)
                        .from(METADATA_TABLE)
                        .fetch();
                Map<String, byte[]> metadata = Maps.newHashMapWithExpectedSize(records.size());
                for (Record record : records) {
                    metadata.put(record.getValue(TABLE_NAME), record.getValue(METADATA));
                }
                return metadata;
            }
        });
    }

    @Override
    public void putMetadataForTable(String tableName, byte[] metadata) {
        putMetadataForTables(ImmutableMap.of(tableName, metadata));
    }

    @Override
    public void putMetadataForTables(final Map<String, byte[]> tableNameToMetadata) {
        if (tableNameToMetadata.isEmpty()) {
            return;
        }
        run(new Function<DSLContext, Void>() {
            @Override
            public Void apply(DSLContext ctx) {
                Query query = ctx
                        .update(METADATA_TABLE)
                        .set(METADATA, (byte[]) null)
                        .where(TABLE_NAME.eq((String) null));
                BatchBindStep batch = ctx.batch(query);
                for (Entry<String, byte[]> entry : tableNameToMetadata.entrySet()) {
                    batch = batch.bind(entry.getValue(), entry.getKey());
                }
                batch.execute();
                return null;
            }
        });
    }

    @Override
    public void compactInternally(final String tableName) {
        if (sqlDialect.family() == SQLDialect.POSTGRES) {
            run(new Function<DSLContext, Void>() {
                @Override
                public Void apply(DSLContext ctx) {
                    ctx.execute("VACUUM ANALYZE " + tableName(tableName));
                    return null;
                }
            });
        }
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

    @Override
    public void teardown() {
        close();
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
            return ctx.transactionResult(new TransactionalCallable<T>() {
                @Override
                public T run(Configuration configuration) throws Exception {
                    return fun.apply(ctx);
                }
            });
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
    String tableName(String rawName) {
        String hash = hashTableName(rawName).substring(0, 8);
        if (tablePrefix.length() + rawName.length() + hash.length() < 30) {
            return tablePrefix + rawName.replace('.', '_') + '_' + hash;
        } else {
            String fullName = tablePrefix + rawName.replace('.', '_');
            return fullName.substring(0, 10) + '_' + fullName.substring(fullName.length() - 10) + '_' + hash;
        }
    }

    Table<Record> atlasTable(String tableName) {
        return table(tableName(tableName));
    }

    String primaryKey(String rawName) {
        String hash = hashTableName(rawName).substring(0, 16);
        return "pk_" + tablePrefix + hash;
    }

    String hashTableName(String rawName) {
        // Take 40 bits from the raw name
        byte[] bytes = Hashing.murmur3_128().hashString(rawName, StandardCharsets.UTF_8).asBytes();
        return BaseEncoding.base32().omitPadding().encode(bytes);
    }
}
