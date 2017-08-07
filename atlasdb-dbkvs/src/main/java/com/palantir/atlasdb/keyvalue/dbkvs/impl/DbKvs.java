/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Suppliers;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Atomics;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.ClusterAvailabilityStatus;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.dbkvs.DbKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.DdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.H2DdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutablePostgresDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleTableNameGetter;
import com.palantir.atlasdb.keyvalue.dbkvs.PostgresDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.batch.AccumulatorStrategies;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.batch.BatchingStrategies;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.batch.BatchingTaskRunner;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.batch.ImmediateSingleBatchTaskRunner;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.batch.ParallelTaskRunner;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.OracleGetRange;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.OracleOverflowValueLoader;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.postgres.DbkvsVersionException;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.postgres.PostgresGetRange;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.postgres.PostgresPrefixedTableNames;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ranges.DbKvsGetRange;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ranges.DbKvsGetRanges;
import com.palantir.atlasdb.keyvalue.dbkvs.util.DbKvsPartitioners;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.keyvalue.impl.GetCandidateCellsForSweepingShim;
import com.palantir.atlasdb.keyvalue.impl.LocalRowColumnRangeIterator;
import com.palantir.common.annotation.Output;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.sql.AgnosticLightResultRow;
import com.palantir.nexus.db.sql.AgnosticResultSet;
import com.palantir.nexus.db.sql.SqlConnection;
import com.palantir.util.crypto.Sha256Hash;
import com.palantir.util.paging.AbstractPagingIterable;
import com.palantir.util.paging.SimpleTokenBackedResultsPage;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

public final class DbKvs extends AbstractKeyValueService {
    private static final Logger log = LoggerFactory.getLogger(DbKvs.class);

    public static final String ROW = "row_name";
    public static final String COL = "col_name";
    public static final String TIMESTAMP = "ts";
    public static final String VAL = "val";
    public static final long DEFAULT_GET_RANGE_OF_TS_BATCH = 1_000_000L;

    private long maxRangeOfTimestampsBatchSize = DEFAULT_GET_RANGE_OF_TS_BATCH;

    private final DdlConfig config;
    private final DbTableFactory dbTables;
    private final SqlConnectionSupplier connections;
    private final BatchingTaskRunner batchingQueryRunner;
    private final ExecutorService ddlExecutor;
    private final OverflowValueLoader overflowValueLoader;
    private final DbKvsGetRange getRangeStrategy;

    public static DbKvs create(DbKeyValueServiceConfig config, SqlConnectionSupplier sqlConnSupplier) {
        DbKvs dbKvs = createNoInit(config.ddl(), sqlConnSupplier);
        dbKvs.init();
        return dbKvs;
    }

    /**
     * Constructor for a SQL (either Postgres or Oracle) backed key value store.  This method should not
     * be used directly and is exposed to support legacy software.  Instead you should prefer the use of
     * ConnectionManagerAwareDbKvs which will instantiate a properly initialized DbKVS using the above create method
     */
    public static DbKvs createNoInit(DdlConfig config, SqlConnectionSupplier connections) {
        ExecutorService executor = AbstractKeyValueService.createFixedThreadPool(
                "Atlas Relational KVS", config.poolSize());
        return config.accept(new DdlConfig.Visitor<DbKvs>() {
            @Override
            public DbKvs visit(PostgresDdlConfig postgresDdlConfig) {
                return createPostgres(executor, postgresDdlConfig, connections);
            }
            @Override
            public DbKvs visit(H2DdlConfig h2DdlConfig) {
                PostgresDdlConfig postgresDdlConfig = ImmutablePostgresDdlConfig.builder().from(h2DdlConfig).build();
                return createPostgres(executor, postgresDdlConfig, connections);
            }
            @Override
            public DbKvs visit(OracleDdlConfig oracleDdlConfig) {
                return createOracle(executor, oracleDdlConfig, connections);
            }
        });
    }

    private static DbKvs createPostgres(ExecutorService executor,
                                        PostgresDdlConfig config,
                                        SqlConnectionSupplier connections) {
        PostgresPrefixedTableNames prefixedTableNames = new PostgresPrefixedTableNames(config);
        DbTableFactory tableFactory = new PostgresDbTableFactory(config, prefixedTableNames);
        TableMetadataCache tableMetadataCache = new TableMetadataCache(tableFactory);
        return new DbKvs(
                executor,
                config,
                tableFactory,
                connections,
                new ParallelTaskRunner(newFixedThreadPool(config.poolSize()), config.fetchBatchSize()),
                (conns, tbl, ids) -> Collections.emptyMap(), // no overflow on postgres
                new PostgresGetRange(prefixedTableNames, connections, tableMetadataCache));
    }

    private static DbKvs createOracle(ExecutorService executor,
                                      OracleDdlConfig oracleDdlConfig,
                                      SqlConnectionSupplier connections) {
        OracleTableNameGetter tableNameGetter = new OracleTableNameGetter(oracleDdlConfig);
        OraclePrefixedTableNames prefixedTableNames = new OraclePrefixedTableNames(tableNameGetter);
        TableValueStyleCache valueStyleCache = new TableValueStyleCache();
        OverflowValueLoader overflowValueLoader = new OracleOverflowValueLoader(oracleDdlConfig, tableNameGetter);
        DbKvsGetRange getRange = new OracleGetRange(
                connections, overflowValueLoader, tableNameGetter, valueStyleCache, oracleDdlConfig);
        return new DbKvs(
                executor,
                oracleDdlConfig,
                new OracleDbTableFactory(oracleDdlConfig, tableNameGetter, prefixedTableNames, valueStyleCache),
                connections,
                new ImmediateSingleBatchTaskRunner(),
                overflowValueLoader,
                getRange);
    }

    private DbKvs(ExecutorService executor,
                  DdlConfig config,
                  DbTableFactory dbTables,
                  SqlConnectionSupplier connections,
                  BatchingTaskRunner batchingQueryRunner,
                  OverflowValueLoader overflowValueLoader,
                  DbKvsGetRange getRangeStrategy) {
        super(executor);
        this.config = config;
        this.dbTables = dbTables;
        this.connections = connections;
        ddlExecutor = Executors.newFixedThreadPool(
                config.parallelDdlOperationConcurrency(),
                new NamedThreadFactory("Atlas Relational KVS DDL Runner"));
        this.batchingQueryRunner = batchingQueryRunner;
        this.overflowValueLoader = overflowValueLoader;
        this.getRangeStrategy = getRangeStrategy;
    }

    private static ThreadPoolExecutor newFixedThreadPool(int maxPoolSize) {
        ThreadPoolExecutor pool = PTExecutors.newThreadPoolExecutor(maxPoolSize, maxPoolSize,
                15L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new NamedThreadFactory("Atlas DbKvs reader", true /* daemon */));

        pool.allowCoreThreadTimeOut(false);
        return pool;
    }

    private void init() {
        checkDatabaseVersion();
        databaseSpecificInitialization();
        createMetadataTable();
    }

    private void databaseSpecificInitialization() {
        runInitialization(new Function<DbTableInitializer, Void>() {
            @Nullable
            @Override
            public Void apply(@Nonnull DbTableInitializer initializer) {
                initializer.createUtilityTables();
                return null;
            }
        });
    }

    private void createMetadataTable() {
        runInitialization(initializer -> {
            initializer.createMetadataTable(config.metadataTable().getQualifiedName());
            return null;
        });
    }
    @Override
    public void close() {
        super.close();
        dbTables.close();
        connections.close();
        batchingQueryRunner.close();
        ddlExecutor.shutdown();
    }

    @Override
    public Map<Cell, Value> getRows(
            TableReference tableRef,
            Iterable<byte[]> rows,
            ColumnSelection columnSelection,
            long timestamp) {
        return getRowsBatching(tableRef, rows, columnSelection, timestamp);
    }

    @Override
    public Map<Cell, Value> get(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        return batchingQueryRunner.runTask(
                timestampByCell,
                BatchingStrategies.forMap(),
                AccumulatorStrategies.forMap(),
                cellBatch -> runReadAndExtractResults(tableRef, table ->
                        table.getLatestCells(cellBatch, true)));
    }

    private Map<Cell, Value> getRowsBatching(TableReference tableRef,
                                             Iterable<byte[]> rows,
                                             ColumnSelection columnSelection,
                                             long timestamp) {
        return batchingQueryRunner.runTask(
                rows,
                BatchingStrategies.forIterable(),
                AccumulatorStrategies.forMap(),
                rowBatch -> runReadAndExtractResults(tableRef, table ->
                        table.getLatestRows(rowBatch, columnSelection, timestamp, true)));
    }

    private Map<Cell, Value> runReadAndExtractResults(
            TableReference tableRef,
            Function<DbReadTable, ClosableIterator<AgnosticLightResultRow>> query) {
        return runRead(tableRef, table -> extractResults(table, tableRef, query.apply(table)));
    }

    @SuppressWarnings("deprecation")
    private Map<Cell, Value> extractResults(
            DbReadTable table,
            TableReference tableRef,
            ClosableIterator<AgnosticLightResultRow> rows) {
        Map<Cell, Value> results = Maps.newHashMap();
        Map<Cell, OverflowValue> overflowResults = Maps.newHashMap();
        try (ClosableIterator<AgnosticLightResultRow> iter = rows) {
            boolean hasOverflow = table.hasOverflowValues();
            while (iter.hasNext()) {
                AgnosticLightResultRow row = iter.next();
                Cell cell = Cell.create(row.getBytes(ROW), row.getBytes(COL));
                Long overflowId = hasOverflow ? row.getLongObject("overflow") : null;
                if (overflowId == null) {
                    Value value = Value.create(row.getBytes(VAL), row.getLong(TIMESTAMP));
                    Value oldValue = results.put(cell, value);
                    if (oldValue != null && oldValue.getTimestamp() > value.getTimestamp()) {
                        results.put(cell, oldValue);
                    }
                } else {
                    OverflowValue ov = ImmutableOverflowValue.of(row.getLong(TIMESTAMP), overflowId);
                    OverflowValue oldOv = overflowResults.put(cell, ov);
                    if (oldOv != null && oldOv.ts() > ov.ts()) {
                        overflowResults.put(cell, oldOv);
                    }
                }
            }
        }
        fillOverflowValues(table.getConnectionSupplier(), tableRef, overflowResults, results);
        return results;
    }

    @Override
    public Map<Cell, Long> getLatestTimestamps(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        return batchingQueryRunner.runTask(
                timestampByCell,
                BatchingStrategies.forMap(),
                AccumulatorStrategies.forMap(),
                cellBatch -> runRead(tableRef, table -> doGetLatestTimestamps(table, cellBatch)));
    }

    private static Map<Cell, Long> doGetLatestTimestamps(DbReadTable table, Map<Cell, Long> timestampByCell) {
        try (ClosableIterator<AgnosticLightResultRow> iter = table.getLatestCells(timestampByCell, false)) {
            Map<Cell, Long> results = Maps.newHashMap();
            while (iter.hasNext()) {
                AgnosticLightResultRow row = iter.next();
                Cell cell = Cell.create(row.getBytes(ROW), row.getBytes(COL));
                long ts = row.getLong(TIMESTAMP);
                Long oldTs = results.put(cell, ts);
                if (oldTs != null && oldTs > ts) {
                    results.put(cell, oldTs);
                }
            }
            return results;
        }
    }

    public Function<Entry<Cell, byte[]>, Long> getByteSizingFunction() {
        return entry -> Cells.getApproxSizeOfCell(entry.getKey()) + entry.getValue().length;
    }

    public Function<Entry<Cell, Value>, Long> getValueSizingFunction() {
        return entry -> Cells.getApproxSizeOfCell(entry.getKey()) + entry.getValue().getContents().length;
    }

    private void put(TableReference tableRef, Map<Cell, byte[]> values, long timestamp, boolean idempotent) {
        Iterable<List<Entry<Cell, byte[]>>> batches = partitionByCountAndBytes(
                values.entrySet(),
                config.mutationBatchCount(),
                config.mutationBatchSizeBytes(),
                tableRef,
                getByteSizingFunction());

        runReadWrite(tableRef, (readTable, writeTable) -> {
            for (List<Entry<Cell, byte[]>> batch : batches) {
                try {
                    writeTable.put(batch, timestamp);
                } catch (KeyAlreadyExistsException e) {
                    if (idempotent) {
                        putIfNotUpdate(readTable, writeTable, tableRef, batch, timestamp, e);
                    } else {
                        throw e;
                    }
                }
            }
            return null;
        });
    }

    @Override
    public void put(TableReference tableRef, Map<Cell, byte[]> values, long timestamp)
            throws KeyAlreadyExistsException {
        put(tableRef, values, timestamp, true);
    }

    private void putIfNotUpdate(
            DbReadTable readTable,
            DbWriteTable writeTable,
            TableReference tableRef,
            List<Entry<Cell, Value>> batch,
            KeyAlreadyExistsException ex) {
        Map<Cell, Long> timestampByCell = Maps.newHashMap();
        for (Entry<Cell, Value> entry : batch) {
            timestampByCell.put(entry.getKey(), entry.getValue().getTimestamp() + 1);
        }

        Map<Cell, Value> results = extractResults(readTable, tableRef, readTable.getLatestCells(timestampByCell, true));

        ListIterator<Entry<Cell, Value>> iter = batch.listIterator();
        while (iter.hasNext()) {
            Entry<Cell, Value> entry = iter.next();
            Cell key = entry.getKey();
            Value value = entry.getValue();
            if (results.containsKey(key)) {
                if (results.get(key).equals(value)) {
                    iter.remove();
                } else {
                    throw new KeyAlreadyExistsException(
                            "primary key violation for key " + key + " with value " + value,
                            ex);
                }
            }
        }
        writeTable.put(batch);
    }

    private void putIfNotUpdate(
            DbReadTable readTable,
            DbWriteTable writeTable,
            TableReference tableRef,
            List<Entry<Cell, byte[]>> batch,
            long timestamp,
            KeyAlreadyExistsException ex) {
        List<Entry<Cell, Value>> batchValues =
                Lists.transform(batch,
                        input -> Maps.immutableEntry(input.getKey(), Value.create(input.getValue(), timestamp)));
        putIfNotUpdate(readTable, writeTable, tableRef, batchValues, ex);
    }

    @Override
    public void putWithTimestamps(TableReference tableRef, Multimap<Cell, Value> cellValues)
            throws KeyAlreadyExistsException {
        Iterable<List<Entry<Cell, Value>>> batches = partitionByCountAndBytes(
                cellValues.entries(),
                config.mutationBatchCount(),
                config.mutationBatchSizeBytes(),
                tableRef,
                getValueSizingFunction());

        runReadWrite(tableRef, (readTable, writeTable) -> {
            for (List<Entry<Cell, Value>> batch : batches) {
                try {
                    writeTable.put(batch);
                } catch (KeyAlreadyExistsException e) {
                    putIfNotUpdate(readTable, writeTable, tableRef, batch, e);
                }
            }
            return null;
        });
    }

    @Override
    public void putUnlessExists(TableReference tableRef, Map<Cell, byte[]> values) throws KeyAlreadyExistsException {
        put(tableRef, values, AtlasDbConstants.TRANSACTION_TS, false);
    }

    @Override
    public void checkAndSet(CheckAndSetRequest checkAndSetRequest) throws CheckAndSetException {
        if (checkAndSetRequest.oldValue().isPresent()) {
            executeCheckAndSet(checkAndSetRequest);
        } else {
            executePutUnlessExists(checkAndSetRequest);
        }
    }

    private void executeCheckAndSet(CheckAndSetRequest request) {
        Preconditions.checkArgument(request.oldValue().isPresent());

        runWrite(request.table(), table -> {
            //noinspection OptionalGetWithoutIsPresent
            table.update(request.cell(), AtlasDbConstants.TRANSACTION_TS, request.oldValue().get(), request.newValue());
            return null;
        });
    }

    private void executePutUnlessExists(CheckAndSetRequest checkAndSetRequest) {
        try {
            Map<Cell, byte[]> value = ImmutableMap.of(checkAndSetRequest.cell(), checkAndSetRequest.newValue());
            putUnlessExists(checkAndSetRequest.table(), value);
        } catch (KeyAlreadyExistsException e) {
            throw new CheckAndSetException("Value unexpectedly present when running check and set", e);
        }
    }

    @Override
    public void delete(TableReference tableRef, Multimap<Cell, Long> keys) {
        // QA-86494: We sort our deletes here because we have seen oracle deadlock errors here.
        ImmutableList<Entry<Cell, Long>> sorted = ORDERING.immutableSortedCopy(keys.entries());
        Iterable<List<Entry<Cell, Long>>> partitions = partitionByCountAndBytes(
                sorted,
                10000,
                getMultiPutBatchSizeBytes(),
                tableRef,
                entry -> Cells.getApproxSizeOfCell(entry.getKey()) + 8);
        runWriteForceAutocommit(tableRef, new Function<DbWriteTable, Void>() {
            @Override
            public Void apply(DbWriteTable table) {
                for (List<Entry<Cell, Long>> partition : partitions) {
                    table.delete(partition);
                }
                return null;
            }
        });
    }

    private static final Ordering<Entry<Cell, Long>> ORDERING = Ordering.from(new Comparator<Entry<Cell, Long>>() {
        @Override
        public int compare(Entry<Cell, Long> entry1, Entry<Cell, Long> entry2) {
            int comparison = Ordering.natural().compare(entry1.getKey(), entry2.getKey());
            if (comparison == 0) {
                comparison = Ordering.natural().compare(entry1.getValue(), entry2.getValue());
            }
            return comparison;
        }
    });

    @Override
    public void deleteRange(TableReference tableRef, RangeRequest range) {
        runWriteForceAutocommit(tableRef, table -> {
            table.delete(range);
            return null;
        });
    }

    @Override
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(
            TableReference tableRef,
            Iterable<RangeRequest> rangeRequests,
            long timestamp) {
        return new DbKvsGetRanges(this, dbTables.getDbType(), connections, dbTables.getPrefixedTableNames())
                .getFirstBatchForRanges(tableRef, rangeRequests, timestamp);
    }

    @Override
    public ClosableIterator<RowResult<Value>> getRange(
            TableReference tableRef,
            RangeRequest rangeRequest,
            long timestamp) {
        return ClosableIterators.wrap(getRangeStrategy.getRange(tableRef, rangeRequest, timestamp));
    }

    public void setMaxRangeOfTimestampsBatchSize(long newValue) {
        maxRangeOfTimestampsBatchSize = newValue;
    }

    public long getMaxRangeOfTimestampsBatchSize() {
        return maxRangeOfTimestampsBatchSize;
    }

    /**
     * @param tableRef the name of the table to read from.
     * @param rangeRequest the range to load.
     * @param timestamp the maximum timestamp to load.
     *
     * @return Each row that has fewer than maxRangeOfTimestampsBatchSize entries is guaranteed to be returned in a
     * single RowResult. If a row has more than maxRangeOfTimestampsBatchSize results, it will potentially be split
     * into multiple RowResults, by finishing the current column; see example below. Note that:
     *  1) this may cause a RowResult to have more than maxRangeOfTimestampsBatchSize entries
     *  2) this may still finish a row, in which case there is going to be only one RowResult for that row.
     * It is, furthermore,  guaranteed that the columns will be read in ascending order
     *
     * E.g., for the following table, rangeRequest taking all rows in ascending order,
     * maxRangeOfTimestampsBatchSize == 5, and timestamp 10:
     *
     *           a     |     b     |     c     |     d
     *     ------------------------------------------------
     *   a | (1, 2, 3) | (1, 2, 3) | (4, 5, 6) | (4, 5, 6)|
     *     ------------------------------------------------
     *   b | (1, 3, 5) |     -     | (1)       |     -    |
     *     ------------------------------------------------
     *   c | (1, 2)    | (1, 2)    | (4, 5, 6) | (4, 5, 6)|
     *     ------------------------------------------------
     *   d | (1, 3, 5) |     -     | (1, 2, 3) |     -    |
     *     ------------------------------------------------
     *   e | (1, 3)    |     -     |     -     |     -    |
     *     ------------------------------------------------
     *
     * The RowResults will be:
     *   1. (a, (a -> 1, 2, 3; b -> 1, 2, 3))
     *   2. (a, (c -> 4, 5, 6; d -> 4, 5, 6))
     *
     *   3. (b, (a -> 1, 3, 5; b -> 1))
     *
     *   4. (c, (a -> 1, 2, b -> 1, 2; c -> 4, 5, 6))
     *   5. (c, (d -> 4, 5, 6))
     *
     *   6. (d, (a -> 1, 3, 5, c -> 1, 2, 3))
     *
     *   7. (e, (a -> 1, 3))
     */
    @Override
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(
            TableReference tableRef,
            RangeRequest rangeRequest,
            long timestamp) {
        Iterable<RowResult<Set<Long>>> rows = new AbstractPagingIterable<
                RowResult<Set<Long>>,
                TokenBackedBasicResultsPage<RowResult<Set<Long>>, Token>>() {
            @Override
            protected TokenBackedBasicResultsPage<RowResult<Set<Long>>, Token> getFirstPage() {
                return getTimestampsPage(tableRef,
                        rangeRequest,
                        timestamp,
                        maxRangeOfTimestampsBatchSize,
                        Token.INITIAL);
            }

            @Override
            protected TokenBackedBasicResultsPage<RowResult<Set<Long>>, Token> getNextPage(
                    TokenBackedBasicResultsPage<RowResult<Set<Long>>, Token> previous) {
                Token token = previous.getTokenForNextPage();
                RangeRequest newRange = rangeRequest.getBuilder().startRowInclusive(token.row()).build();
                return getTimestampsPage(tableRef, newRange, timestamp, maxRangeOfTimestampsBatchSize, token);
            }
        };
        return ClosableIterators.wrap(rows.iterator());
    }

    @Override
    public ClosableIterator<List<CandidateCellForSweeping>> getCandidateCellsForSweeping(TableReference tableRef,
            CandidateCellForSweepingRequest request) {
        return new GetCandidateCellsForSweepingShim(this).getCandidateCellsForSweeping(tableRef, request);
    }

    private TokenBackedBasicResultsPage<RowResult<Set<Long>>, Token> getTimestampsPage(
            TableReference tableRef,
            RangeRequest range,
            long timestamp,
            long batchSize,
            Token token) {
        Stopwatch watch = Stopwatch.createStarted();
        try {
            return runRead(tableRef, table -> getTimestampsPageInternal(table, range, timestamp, batchSize, token));
        } finally {
            log.debug("Call to KVS.getTimestampsPage on table {} took {} ms.",
                    tableRef, watch.elapsed(TimeUnit.MILLISECONDS));
        }
    }

    private TokenBackedBasicResultsPage<RowResult<Set<Long>>, Token> getTimestampsPageInternal(
            DbReadTable table,
            RangeRequest range,
            long timestamp,
            long batchSize,
            Token token) {
        Set<byte[]> rows = Sets.newHashSet();
        int maxRows = getMaxRowsFromBatchHint(range.getBatchHint());

        try (ClosableIterator<AgnosticLightResultRow> rangeResults = table.getRange(range, timestamp, maxRows)) {
            while (rows.size() < maxRows && rangeResults.hasNext()) {
                byte[] rowName = rangeResults.next().getBytes(ROW);
                if (rowName != null) {
                    rows.add(rowName);
                }
            }
            if (rows.isEmpty()) {
                return SimpleTokenBackedResultsPage.create(null, ImmutableList.<RowResult<Set<Long>>>of(), false);
            }
        }

        ColumnSelection cols = range.getColumnNames().isEmpty()
                ? ColumnSelection.all() : ColumnSelection.create(range.getColumnNames());

        TimestampsByCellResultWithToken result = getTimestampsByCell(table,
                rows,
                cols,
                timestamp,
                batchSize,
                range.isReverse(),
                token);

        NavigableMap<byte[], SortedMap<byte[], Set<Long>>> cellsByRow =
                Cells.breakCellsUpByRow(Multimaps.asMap(result.entries));
        if (range.isReverse()) {
            cellsByRow = cellsByRow.descendingMap();
        }
        List<RowResult<Set<Long>>> finalResults = cellsByRow.entrySet().stream()
                .map(entry -> RowResult.create(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());

        return SimpleTokenBackedResultsPage.create(result.getToken(), finalResults, result.mayHaveMoreResults());
    }

    private TimestampsByCellResultWithToken getTimestampsByCell(
            DbReadTable table,
            Iterable<byte[]> rows,
            ColumnSelection columns,
            long timestamp,
            long batchSize,
            boolean isReverse,
            Token token) {
        try (ClosableIterator<AgnosticLightResultRow> iterator = table
                .getAllRows(rows, columns, timestamp, false, DbReadTable.Order.fromBoolean(isReverse))) {
            return TimestampsByCellResultWithToken.create(iterator, token, batchSize, isReverse);
        }
    }

    @Override
    public Map<byte[], RowColumnRangeIterator> getRowsColumnRange(
            TableReference tableRef,
            Iterable<byte[]> rows,
            BatchColumnRangeSelection batchColumnRangeSelection,
            long timestamp) {
        List<byte[]> rowList = ImmutableList.copyOf(rows);
        Map<byte[], List<Map.Entry<Cell, Value>>> firstPage =
                getFirstRowsColumnRangePage(tableRef, rowList, batchColumnRangeSelection, timestamp);

        Map<byte[], RowColumnRangeIterator> ret = Maps.newHashMapWithExpectedSize(rowList.size());
        for (Entry<byte[], List<Map.Entry<Cell, Value>>> e : firstPage.entrySet()) {
            List<Map.Entry<Cell, Value>> results = e.getValue();
            if (results.isEmpty()) {
                ret.put(e.getKey(), new LocalRowColumnRangeIterator(e.getValue().iterator()));
                continue;
            }
            byte[] lastCol = results.get(results.size() - 1).getKey().getColumnName();
            RowColumnRangeIterator firstPageIter = new LocalRowColumnRangeIterator(e.getValue().iterator());
            if (isEndOfColumnRange(lastCol, batchColumnRangeSelection.getEndCol())) {
                ret.put(e.getKey(), firstPageIter);
            } else {
                byte[] nextCol = RangeRequests.nextLexicographicName(lastCol);
                BatchColumnRangeSelection nextColumnRangeSelection =
                        BatchColumnRangeSelection.create(
                                nextCol,
                                batchColumnRangeSelection.getEndCol(),
                                batchColumnRangeSelection.getBatchHint());
                Iterator<Map.Entry<Cell, Value>> nextPagesIter = getRowColumnRange(
                        tableRef,
                        e.getKey(),
                        nextColumnRangeSelection,
                        timestamp);
                ret.put(e.getKey(), new LocalRowColumnRangeIterator(Iterators.concat(firstPageIter, nextPagesIter)));
            }
        }
        return ret;
    }

    @Override
    public RowColumnRangeIterator getRowsColumnRange(TableReference tableRef,
                                                     Iterable<byte[]> rows,
                                                     ColumnRangeSelection columnRangeSelection,
                                                     int cellBatchHint,
                                                     long timestamp) {
        List<byte[]> rowList = ImmutableList.copyOf(rows);
        Map<Sha256Hash, byte[]> rowHashesToBytes = Maps.uniqueIndex(rowList, Sha256Hash::computeHash);

        Map<Sha256Hash, Integer> columnCountByRowHash =
                getColumnCounts(tableRef, rowList, columnRangeSelection, timestamp);

        Iterator<Map<Sha256Hash, Integer>> batches =
                DbKvsPartitioners.partitionByTotalCount(columnCountByRowHash, cellBatchHint).iterator();
        Iterator<Iterator<Map.Entry<Cell, Value>>> results =
                loadColumnsForBatches(tableRef,
                                      columnRangeSelection,
                                      timestamp,
                                      rowHashesToBytes,
                                      batches,
                                      columnCountByRowHash);
        return new LocalRowColumnRangeIterator(Iterators.concat(results));
    }

    private Iterator<Iterator<Map.Entry<Cell, Value>>> loadColumnsForBatches(
            TableReference tableRef,
            ColumnRangeSelection columnRangeSelection,
            long timestamp,
            Map<Sha256Hash, byte[]> rowHashesToBytes,
            Iterator<Map<Sha256Hash, Integer>> batches,
            Map<Sha256Hash, Integer> columnCountByRowHash) {
        Iterator<Iterator<Map.Entry<Cell, Value>>> results = new AbstractIterator<Iterator<Map.Entry<Cell, Value>>>() {
            private Sha256Hash lastRowHashInPreviousBatch = null;
            private byte[] lastColumnInPreviousBatch = null;

            @Override
            protected Iterator<Map.Entry<Cell, Value>> computeNext() {
                if (!batches.hasNext()) {
                    return endOfData();
                }
                Map<Sha256Hash, Integer> currentBatch = batches.next();
                RowsColumnRangeBatchRequest columnRangeSelectionsByRow =
                        getBatchColumnRangeSelectionsByRow(currentBatch, columnCountByRowHash);

                Map<byte[], List<Map.Entry<Cell, Value>>> resultsByRow =
                            extractRowColumnRangePage(tableRef, columnRangeSelectionsByRow, timestamp);
                int totalEntries = resultsByRow.values().stream().mapToInt(List::size).sum();
                if (totalEntries == 0) {
                    return Collections.emptyIterator();
                }
                // Ensure order matches that of the provided batch.
                List<Map.Entry<Cell, Value>> loadedColumns = new ArrayList<>(totalEntries);
                for (Sha256Hash rowHash : currentBatch.keySet()) {
                    byte[] row = rowHashesToBytes.get(rowHash);
                    loadedColumns.addAll(resultsByRow.get(row));
                }

                Cell lastCell = Iterables.getLast(loadedColumns).getKey();
                lastRowHashInPreviousBatch = Sha256Hash.computeHash(lastCell.getRowName());
                lastColumnInPreviousBatch = lastCell.getColumnName();

                return loadedColumns.iterator();
            }

            private RowsColumnRangeBatchRequest getBatchColumnRangeSelectionsByRow(
                    Map<Sha256Hash, Integer> columnCountsByRowHashInBatch,
                    Map<Sha256Hash, Integer> totalColumnCountsByRowHash) {
                ImmutableRowsColumnRangeBatchRequest.Builder rowsColumnRangeBatch =
                        ImmutableRowsColumnRangeBatchRequest.builder().columnRangeSelection(columnRangeSelection);
                Iterator<Map.Entry<Sha256Hash, Integer>> entries = columnCountsByRowHashInBatch.entrySet().iterator();
                while (entries.hasNext()) {
                    Map.Entry<Sha256Hash, Integer> entry = entries.next();
                    Sha256Hash rowHash = entry.getKey();
                    byte[] row = rowHashesToBytes.get(rowHash);
                    boolean isPartialFirstRow = Objects.equals(lastRowHashInPreviousBatch, rowHash);
                    if (isPartialFirstRow) {
                        byte[] startCol = RangeRequests.nextLexicographicName(lastColumnInPreviousBatch);
                        BatchColumnRangeSelection columnRange =
                                BatchColumnRangeSelection.create(
                                        startCol,
                                        columnRangeSelection.getEndCol(),
                                        entry.getValue());
                        rowsColumnRangeBatch.partialFirstRow(Maps.immutableEntry(row, columnRange));
                        continue;
                    }
                    boolean isFullyLoadedRow = totalColumnCountsByRowHash.get(rowHash).equals(entry.getValue());
                    if (isFullyLoadedRow) {
                        rowsColumnRangeBatch.addRowsToLoadFully(row);
                    } else {
                        Preconditions.checkArgument(!entries.hasNext(), "Only the last row should be partial.");
                        BatchColumnRangeSelection columnRange =
                                BatchColumnRangeSelection.create(columnRangeSelection, entry.getValue());
                        rowsColumnRangeBatch.partialLastRow(Maps.immutableEntry(row, columnRange));
                    }
                }
                return rowsColumnRangeBatch.build();
            }
        };
        return results;
    }

    private Iterator<Map.Entry<Cell, Value>> getRowColumnRange(
            TableReference tableRef,
            byte[] row,
            BatchColumnRangeSelection batchColumnRangeSelection,
            long timestamp) {
        List<byte[]> rowList = ImmutableList.of(row);
        return ClosableIterators.wrap(new AbstractPagingIterable<
                Entry<Cell, Value>,
                TokenBackedBasicResultsPage<Entry<Cell, Value>, byte[]>>() {
            @Override
            protected TokenBackedBasicResultsPage<Entry<Cell, Value>, byte[]> getFirstPage() throws Exception {
                return page(batchColumnRangeSelection.getStartCol());
            }

            @Override
            protected TokenBackedBasicResultsPage<Map.Entry<Cell, Value>, byte[]> getNextPage(
                    TokenBackedBasicResultsPage<Map.Entry<Cell, Value>, byte[]> previous) throws Exception {
                return page(previous.getTokenForNextPage());
            }

            TokenBackedBasicResultsPage<Map.Entry<Cell, Value>, byte[]> page(byte[] startCol) throws Exception {
                BatchColumnRangeSelection range = BatchColumnRangeSelection.create(
                                startCol,
                                batchColumnRangeSelection.getEndCol(),
                                batchColumnRangeSelection.getBatchHint());
                List<Map.Entry<Cell, Value>> nextPage = Iterables.getOnlyElement(
                        extractRowColumnRangePage(tableRef, range, timestamp, rowList).values());
                if (nextPage.isEmpty()) {
                    return SimpleTokenBackedResultsPage.create(startCol, ImmutableList.<Entry<Cell, Value>>of(), false);
                }
                byte[] lastCol = nextPage.get(nextPage.size() - 1).getKey().getColumnName();
                if (isEndOfColumnRange(lastCol, batchColumnRangeSelection.getEndCol())) {
                    return SimpleTokenBackedResultsPage.create(lastCol, nextPage, false);
                }
                byte[] nextCol = RangeRequests.nextLexicographicName(lastCol);
                return SimpleTokenBackedResultsPage.create(nextCol, nextPage, true);
            }

        }.iterator());
    }

    private boolean isEndOfColumnRange(byte[] lastCol, byte[] endCol) {
        return RangeRequests.isLastRowName(lastCol)
                || Arrays.equals(RangeRequests.nextLexicographicName(lastCol), endCol);
    }

    private Map<byte[], List<Map.Entry<Cell, Value>>> getFirstRowsColumnRangePage(
            TableReference tableRef,
            List<byte[]> rows,
            BatchColumnRangeSelection columnRangeSelection,
            long ts) {
        Stopwatch watch = Stopwatch.createStarted();
        try {
            return extractRowColumnRangePage(tableRef, columnRangeSelection, ts, rows);
        } finally {
            log.debug("Call to KVS.getFirstRowColumnRangePage on table {} took {} ms.",
                    tableRef, watch.elapsed(TimeUnit.MILLISECONDS));
        }
    }

    private Map<byte[], List<Entry<Cell, Value>>> extractRowColumnRangePage(
            TableReference tableRef,
            BatchColumnRangeSelection columnRangeSelection,
            long ts,
            List<byte[]> rows) {
        return extractRowColumnRangePage(tableRef, Maps.toMap(rows, Functions.constant(columnRangeSelection)), ts);
    }

    private Map<byte[], List<Entry<Cell, Value>>> extractRowColumnRangePage(
            TableReference tableRef,
            Map<byte[], BatchColumnRangeSelection> columnRangeSelection,
            long ts) {
        return batchingQueryRunner.runTask(
            columnRangeSelection,
            BatchingStrategies.forMap(),
            AccumulatorStrategies.forMap(),
            batch -> runRead(tableRef, table ->
                    extractRowColumnRangePageInternal(
                        table,
                        tableRef,
                        () -> table.getRowsColumnRange(batch, ts),
                        batch.keySet())));
    }

    private Map<byte[], List<Entry<Cell, Value>>> extractRowColumnRangePage(
            TableReference tableRef,
            RowsColumnRangeBatchRequest rowsColumnRangeBatch,
            long ts) {
        return batchingQueryRunner.runTask(
            rowsColumnRangeBatch,
            RowsColumnRangeBatchRequests::partition,
            AccumulatorStrategies.forMap(),
            batch -> runRead(tableRef, table ->
                    extractRowColumnRangePageInternal(
                        table,
                        tableRef,
                        () -> table.getRowsColumnRange(batch, ts),
                        RowsColumnRangeBatchRequests.getAllRowsInOrder(batch))));
    }

    private Map<byte[], List<Map.Entry<Cell, Value>>> extractRowColumnRangePageInternal(
            DbReadTable table,
            TableReference tableRef,
            Supplier<ClosableIterator<AgnosticLightResultRow>> rowLoader,
            Collection<byte[]> allRows) {
        Map<Sha256Hash, byte[]> hashesToBytes = Maps.newHashMapWithExpectedSize(allRows.size());
        Map<Sha256Hash, List<Cell>> cellsByRow = Maps.newHashMap();
        for (byte[] row : allRows) {
            Sha256Hash rowHash = Sha256Hash.computeHash(row);
            hashesToBytes.put(rowHash, row);
            cellsByRow.put(rowHash, Lists.newArrayList());
        }

        boolean hasOverflow = table.hasOverflowValues();
        Map<Cell, Value> values = Maps.newHashMap();
        Map<Cell, OverflowValue> overflowValues = Maps.newHashMap();

        try (ClosableIterator<AgnosticLightResultRow> iter = rowLoader.get()) {
            while (iter.hasNext()) {
                AgnosticLightResultRow row = iter.next();
                Cell cell = Cell.create(row.getBytes(ROW), row.getBytes(COL));
                Sha256Hash rowHash = Sha256Hash.computeHash(cell.getRowName());
                cellsByRow.get(rowHash).add(cell);
                Long overflowId = hasOverflow ? row.getLongObject("overflow") : null;
                if (overflowId == null) {
                    Value value = Value.create(row.getBytes(VAL), row.getLong(TIMESTAMP));
                    Value oldValue = values.put(cell, value);
                    if (oldValue != null && oldValue.getTimestamp() > value.getTimestamp()) {
                        values.put(cell, oldValue);
                    }
                } else {
                    OverflowValue ov = ImmutableOverflowValue.of(row.getLong(TIMESTAMP), overflowId);
                    OverflowValue oldOv = overflowValues.put(cell, ov);
                    if (oldOv != null && oldOv.ts() > ov.ts()) {
                        overflowValues.put(cell, oldOv);
                    }
                }
            }
        }

        fillOverflowValues(table.getConnectionSupplier(), tableRef, overflowValues, values);

        Map<byte[], List<Map.Entry<Cell, Value>>> results =
                Maps.newHashMapWithExpectedSize(allRows.size());
        for (Entry<Sha256Hash, List<Cell>> e : cellsByRow.entrySet()) {
            List<Map.Entry<Cell, Value>> fullResults = Lists.newArrayListWithExpectedSize(e.getValue().size());
            for (Cell c : e.getValue()) {
                fullResults.add(Iterables.getOnlyElement(ImmutableMap.of(c, values.get(c)).entrySet()));
            }
            results.put(hashesToBytes.get(e.getKey()), fullResults);
        }
        return results;
    }

    private void fillOverflowValues(ConnectionSupplier conns,
                                    TableReference tableRef,
                                    Map<Cell, OverflowValue> overflowValues,
                                    @Output Map<Cell, Value> values) {
        Iterator<Entry<Cell, OverflowValue>> overflowIterator = overflowValues.entrySet().iterator();
        while (overflowIterator.hasNext()) {
            Entry<Cell, OverflowValue> entry = overflowIterator.next();
            Value value = values.get(entry.getKey());
            if (value != null && value.getTimestamp() > entry.getValue().ts()) {
                overflowIterator.remove();
            }
        }
        Map<Long, byte[]> resolvedOverflowValues = overflowValueLoader.loadOverflowValues(
                conns,
                tableRef,
                Collections2.transform(overflowValues.values(), OverflowValue::id));
        for (Entry<Cell, OverflowValue> entry : overflowValues.entrySet()) {
            Cell cell = entry.getKey();
            OverflowValue ov = entry.getValue();
            byte[] val = resolvedOverflowValues.get(ov.id());
            Preconditions.checkNotNull(val, "Failed to load overflow data: cell=%s, overflowId=%s", cell, ov.id());
            values.put(cell, Value.create(val, ov.ts()));
        }
    }

    private Map<Sha256Hash, Integer> getColumnCounts(TableReference tableRef,
                                                     List<byte[]> rowList,
                                                     ColumnRangeSelection columnRangeSelection,
                                                     long timestamp) {
        Map<Sha256Hash, Integer> countsByRow = batchingQueryRunner.runTask(
                rowList,
                BatchingStrategies.forList(),
                AccumulatorStrategies.forMap(),
                partition -> getColumnCountsUnordered(tableRef, partition, columnRangeSelection, timestamp));
        // Make iteration order of the returned map match the provided list.
        Map<Sha256Hash, Integer> ordered = new LinkedHashMap<>(countsByRow.size());
        for (byte[] row : rowList) {
            Sha256Hash rowHash = Sha256Hash.computeHash(row);
            ordered.put(rowHash, countsByRow.getOrDefault(rowHash, 0));
        }
        return ordered;
    }

    private Map<Sha256Hash, Integer> getColumnCountsUnordered(TableReference tableRef,
                                                              List<byte[]> rowList,
                                                              ColumnRangeSelection columnRangeSelection,
                                                              long timestamp) {
        return runRead(tableRef, dbReadTable -> {
            Map<Sha256Hash, Integer> counts = new HashMap<>(rowList.size());
            try (ClosableIterator<AgnosticLightResultRow> iter =
                    dbReadTable.getRowsColumnRangeCounts(rowList, timestamp, columnRangeSelection)) {
                while (iter.hasNext()) {
                    AgnosticLightResultRow row = iter.next();
                    Sha256Hash rowHash = Sha256Hash.computeHash(row.getBytes(ROW));
                    counts.put(rowHash, row.getInteger("column_count"));
                }
            }
            return counts;
        });
    }

    private void runOnTables(Set<TableReference> tableRefs, Consumer<DbDdlTable> runner) {
        List<Future> futures = Lists.newArrayListWithCapacity(tableRefs.size());
        tableRefs.forEach(tableRef -> futures.add(ddlExecutor.submit(() ->
                runDdl(tableRef, runner))));
        waitOnDdlOperations(futures);
    }

    @Override
    public void truncateTable(TableReference tableRef) {
        truncateTables(ImmutableSet.of(tableRef));
    }

    @Override
    public void truncateTables(Set<TableReference> tableRefs) {
        runOnTables(tableRefs, table -> table.truncate());
    }

    @Override
    public void dropTable(TableReference tableRef) {
        dropTables(ImmutableSet.of(tableRef));
    }

    @Override
    public void dropTables(Set<TableReference> tableRefs) {
        runOnTables(tableRefs, table -> table.drop());
    }

    @Override
    public void createTable(TableReference tableRef, byte[] tableMetadata) {
        createTables(ImmutableMap.of(tableRef, tableMetadata));
    }

    @Override
    public void createTables(Map<TableReference, byte[]> tableRefToTableMetadata) {
        Set<Entry<TableReference, byte[]>> entries = tableRefToTableMetadata.entrySet();
        List<Future> futures = Lists.newArrayListWithCapacity(entries.size());

        entries.forEach(entry -> futures.add(ddlExecutor.submit(() ->
                runDdl(entry.getKey(), table -> {
                    table.create(entry.getValue());
                    putMetadataForTable(entry.getKey(), entry.getValue());
                }))));

        waitOnDdlOperations(futures);
    }

    private void waitOnDdlOperations(List<Future> futures) {
        for (Future future : futures) {
            try {
                future.get(config.ddlOperationTimeoutSeconds(), TimeUnit.SECONDS);
            } catch (Exception e) {
                throw Throwables.rewrapAndThrowUncheckedException(e);
            }
        }
    }

    @Override
    public Set<TableReference> getAllTableNames() {
        return run(conn -> {
            AgnosticResultSet results = conn.selectResultSetUnregisteredQuery(
                    "SELECT table_name FROM " + config.metadataTable().getQualifiedName());
            Set<TableReference> ret = Sets.newHashSetWithExpectedSize(results.size());
            results.rows().forEach(row -> ret.add(TableReference.createUnsafe(row.getString("table_name"))));
            return ret;
        });
    }

    @Override
    public byte[] getMetadataForTable(TableReference tableRef) {
        return runMetadata(tableRef, table -> table.getMetadata());
    }

    @Override
    public void putMetadataForTable(TableReference tableRef, byte[] metadata) {
        runMetadata(tableRef, table -> {
            table.putMetadata(metadata);
            return null;
        });
    }

    @Override
    public Map<TableReference, byte[]> getMetadataForTables() {
        return run(conn -> {
            AgnosticResultSet results = conn.selectResultSetUnregisteredQuery(
                    "SELECT table_name, value FROM " + config.metadataTable().getQualifiedName());
            Map<TableReference, byte[]> ret = Maps.newHashMapWithExpectedSize(results.size());
            results.rows().forEach(row ->
                    ret.put(TableReference.createUnsafe(row.getString("table_name")), row.getBytes("value")));
            return ret;
        });
    }

    @Override
    public void addGarbageCollectionSentinelValues(TableReference tableRef, Iterable<Cell> cells) {
        runWrite(tableRef, table -> {
            table.putSentinels(cells);
            return null;
        });
    }

    @Override
    public Multimap<Cell, Long> getAllTimestamps(TableReference tableRef, Set<Cell> cells, long timestamp) {
        return batchingQueryRunner.runTask(
                cells,
                BatchingStrategies.forIterable(),
                AccumulatorStrategies.forListMultimap(),
                cellBatch -> runRead(tableRef, table -> doGetAllTimestamps(table, cellBatch, timestamp)));
    }

    private static Multimap<Cell, Long> doGetAllTimestamps(DbReadTable table, Iterable<Cell> cells, long timestamp) {
        try (ClosableIterator<AgnosticLightResultRow> iter = table.getAllCells(cells, timestamp, false)) {
            Multimap<Cell, Long> results = ArrayListMultimap.create();
            while (iter.hasNext()) {
                AgnosticLightResultRow row = iter.next();
                Cell cell = Cell.create(row.getBytes(ROW), row.getBytes(COL));
                long ts = row.getLong(TIMESTAMP);
                results.put(cell, ts);
            }
            return results;
        }
    }

    @Override
    public void compactInternally(TableReference tableRef) {
        runDdl(tableRef, table -> table.compactInternally());
    }

    @Override
    public ClusterAvailabilityStatus getClusterAvailabilityStatus() {
        try {
            checkDatabaseVersion();
            return ClusterAvailabilityStatus.ALL_AVAILABLE;
        } catch (DbkvsVersionException e) {
            return ClusterAvailabilityStatus.TERMINAL;
        } catch (Exception e) {
            return ClusterAvailabilityStatus.NO_QUORUM_AVAILABLE;
        }
    }

    public void checkDatabaseVersion() {
        runDdl(TableReference.createWithEmptyNamespace(""), table -> table.checkDatabaseVersion());
    }

    public String getTablePrefix() {
        return config.tablePrefix();
    }

    private <T> T run(Function<SqlConnection, T> runner) {
        SqlConnection conn = connections.get();
        try {
            return runner.apply(conn);
        } finally {
            try {
                conn.getUnderlyingConnection().close();
            } catch (Exception e) {
                log.debug("Error occurred trying to close the connection", e);
            }
        }
    }

    private <T> T runMetadata(TableReference tableRef, Function<DbMetadataTable, T> runner) {
        ConnectionSupplier conns = new ConnectionSupplier(connections);
        try {
            /* The metadata table operates only on the fully qualified table reference */
            return runner.apply(dbTables.createMetadata(tableRef, conns));
        } finally {
            conns.close();
        }
    }

    private void runDdl(TableReference tableRef, Consumer<DbDdlTable> runner) {
        ConnectionSupplier conns = new ConnectionSupplier(connections);
        try {
            /* The ddl actions can used both the fully qualified name and the internal name */
            runner.accept(dbTables.createDdl(tableRef, conns));
        } finally {
            conns.close();
        }
    }

    private <T> T runInitialization(Function<DbTableInitializer, T> runner) {
        ConnectionSupplier conns = new ConnectionSupplier(connections);
        try {
            return runner.apply(dbTables.createInitializer(conns));
        } finally {
            conns.close();
        }
    }

    private <T> T runRead(TableReference tableRef, Function<DbReadTable, T> runner) {
        ConnectionSupplier conns = new ConnectionSupplier(connections);
        try {
            return runner.apply(dbTables.createRead(tableRef, conns));
        } finally {
            conns.close();
        }
    }

    private <T> T runWrite(TableReference tableRef, Function<DbWriteTable, T> runner) {
        ConnectionSupplier conns = new ConnectionSupplier(connections);
        try {
            return runner.apply(dbTables.createWrite(tableRef, conns));
        } finally {
            conns.close();
        }
    }

    private <T> T runReadWrite(TableReference tableRef, ReadWriteTask<T> runner) {
        ConnectionSupplier conns = new ConnectionSupplier(connections);
        try {
            return runner.run(
                    dbTables.createRead(tableRef, conns),
                    dbTables.createWrite(tableRef, conns));
        } finally {
            conns.close();
        }
    }

    private <T> T runWriteForceAutocommit(TableReference tableRef, Function<DbWriteTable, T> runner) {
        ConnectionSupplier conns = new ConnectionSupplier(connections);
        try {
            SqlConnection conn = conns.get();
            boolean autocommit;
            try {
                autocommit = conn.getUnderlyingConnection().getAutoCommit();
            } catch (PalantirSqlException e1) {
                throw Throwables.rewrapAndThrowUncheckedException(e1);
            } catch (SQLException e1) {
                throw Throwables.rewrapAndThrowUncheckedException(e1);
            }
            if (!autocommit) {
                return runWriteFreshConnection(conns, tableRef, runner);
            } else {
                return runner.apply(dbTables.createWrite(tableRef, conns));
            }
        } finally {
            conns.close();
        }
    }

    /**
     * Runs with a new connection, in a new thread so we don't reuse the connection we're
     * getting from ReentrantManagedConnectionSupplier.
     * Note that most of DbKvs reuses connections so unlike most other calls,
     * this can block on getting a new connection if the pool is full.
     * To avoid deadlocks or long pauses, use this only when necessary.
     */
    private <T> T runWriteFreshConnection(
            ConnectionSupplier conns, TableReference tableRef, Function<DbWriteTable, T> runner) {
        log.debug("Running in a new thread to turn autocommit on for write");
        AtomicReference<T> result = Atomics.newReference();
        Thread writeThread = new Thread(() -> {
            SqlConnection freshConn = conns.getFresh();
            try {
                result.set(runner.apply(dbTables.createWrite(tableRef,
                        new ConnectionSupplier(Suppliers.ofInstance(freshConn)))));
            } finally {
                try {
                    Connection conn = freshConn.getUnderlyingConnection();
                    if (conn != null) {
                        conn.close();
                    }
                } catch (SQLException e) {
                    log.error("Failed to close db connection performing write with fresh connection.", e);
                }
            }
        });
        writeThread.start();
        try {
            writeThread.join();
        } catch (InterruptedException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
        return result.get();
    }

    private static Integer getMaxRowsFromBatchHint(@Nullable Integer batchHint) {
        return Optional.ofNullable(batchHint)
                .map(x -> (int) (1.1 * x))
                .orElse(100);
    }

    private interface ReadWriteTask<T> {
        T run(DbReadTable readTable, DbWriteTable writeTable);
    }
}
