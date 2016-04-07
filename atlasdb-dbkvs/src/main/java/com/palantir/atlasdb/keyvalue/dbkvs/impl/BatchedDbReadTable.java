package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import java.io.Closeable;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Queues;
import com.palantir.atlasdb.AtlasSystemPropertyManager;
import com.palantir.atlasdb.DeclaredAtlasSystemProperty;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.nexus.db.sql.AgnosticLightResultRow;
import com.palantir.nexus.db.sql.AgnosticLightResultSet;
import com.palantir.nexus.db.sql.PalantirSqlConnection;

public class BatchedDbReadTable extends AbstractDbReadTable {
    private static final Logger log = LoggerFactory.getLogger(BatchedDbReadTable.class);
    private final Executor exec;
    private final AtlasSystemPropertyManager systemProperties;

    protected BatchedDbReadTable(ConnectionSupplier conns,
                                 DbQueryFactory queryFactory,
                                 Executor exec,
                                 AtlasSystemPropertyManager systemProperties) {
        super(conns, queryFactory);
        this.exec = exec;
        this.systemProperties = systemProperties;
    }

    @Override
    public ClosableIterator<AgnosticLightResultRow> getLatestRowsInternal(Iterable<byte[]> rows,
                                                                          ColumnSelection columns,
                                                                          long ts,
                                                                          boolean includeValues) {
        Queue<Future<ClosableIterator<AgnosticLightResultRow>>> futures = Queues.newArrayDeque();
        for (final List<byte[]> batch : Iterables.partition(rows, getBatchSize())) {
            futures.add(submit(exec, queryFactory.getLatestRowsQuery(batch, ts, columns, includeValues)));
        }
        return new LazyClosableIterator<AgnosticLightResultRow>(futures);
    }

    @Override
    public ClosableIterator<AgnosticLightResultRow> getLatestRowsInternal(Map<byte[], Long> rows,
                                                                          ColumnSelection columns,
                                                                          boolean includeValues) {
        Queue<Future<ClosableIterator<AgnosticLightResultRow>>> futures = Queues.newArrayDeque();
        for (final List<Entry<byte[], Long>> batch : Iterables.partition(rows.entrySet(), getBatchSize())) {
            futures.add(submit(exec, queryFactory.getLatestRowsQuery(batch, columns, includeValues)));
        }
        return new LazyClosableIterator<AgnosticLightResultRow>(futures);
    }

    @Override
    public ClosableIterator<AgnosticLightResultRow> getAllRowsInternal(Iterable<byte[]> rows,
                                                                       ColumnSelection columns,
                                                                       long ts,
                                                                       boolean includeValues) {
        Queue<Future<ClosableIterator<AgnosticLightResultRow>>> futures = Queues.newArrayDeque();
        for (final List<byte[]> batch : Iterables.partition(rows, getBatchSize())) {
            futures.add(submit(exec, queryFactory.getAllRowsQuery(batch, ts, columns, includeValues)));
        }
        return new LazyClosableIterator<AgnosticLightResultRow>(futures);
    }

    @Override
    public ClosableIterator<AgnosticLightResultRow> getAllRowsInternal(Map<byte[], Long> rows,
                                                                       ColumnSelection columns,
                                                                       boolean includeValues) {
        Queue<Future<ClosableIterator<AgnosticLightResultRow>>> futures = Queues.newArrayDeque();
        for (final List<Entry<byte[], Long>> batch : Iterables.partition(rows.entrySet(), getBatchSize())) {
            futures.add(submit(exec, queryFactory.getAllRowsQuery(batch, columns, includeValues)));
        }
        return new LazyClosableIterator<AgnosticLightResultRow>(futures);
    }

    @Override
    public ClosableIterator<AgnosticLightResultRow> getLatestCellsInternal(Iterable<Cell> cells,
                                                                           long ts,
                                                                           boolean includeValue) {
        Queue<Future<ClosableIterator<AgnosticLightResultRow>>> futures = Queues.newArrayDeque();
        for (final List<Cell> batch : Iterables.partition(cells, getBatchSize())) {
            futures.add(submit(exec, queryFactory.getLatestCellsQuery(batch, ts, includeValue)));
        }
        return new LazyClosableIterator<AgnosticLightResultRow>(futures);
    }

    @Override
    public ClosableIterator<AgnosticLightResultRow> getLatestCellsInternal(Map<Cell, Long> cells,
                                                                           boolean includeValue) {
        Queue<Future<ClosableIterator<AgnosticLightResultRow>>> futures = Queues.newArrayDeque();
        for (final List<Entry<Cell, Long>> batch : Iterables.partition(cells.entrySet(), getBatchSize())) {
            futures.add(submit(exec, queryFactory.getLatestCellsQuery(batch, includeValue)));
        }
        return new LazyClosableIterator<AgnosticLightResultRow>(futures);
    }

    @Override
    public ClosableIterator<AgnosticLightResultRow> getAllCellsInternal(Iterable<Cell> cells,
                                                                        long ts,
                                                                        boolean includeValue) {
        Queue<Future<ClosableIterator<AgnosticLightResultRow>>> futures = Queues.newArrayDeque();
        for (final List<Cell> batch : Iterables.partition(cells, getBatchSize())) {
            futures.add(submit(exec, queryFactory.getAllCellsQuery(batch, ts, includeValue)));
        }
        return new LazyClosableIterator<AgnosticLightResultRow>(futures);
    }

    @Override
    public ClosableIterator<AgnosticLightResultRow> getAllCellsInternal(Map<Cell, Long> cells,
                                                                        boolean includeValue) {
        Queue<Future<ClosableIterator<AgnosticLightResultRow>>> futures = Queues.newArrayDeque();
        for (final List<Entry<Cell, Long>> batch : Iterables.partition(cells.entrySet(), getBatchSize())) {
            futures.add(submit(exec, queryFactory.getAllCellsQuery(batch, includeValue)));
        }
        return new LazyClosableIterator<AgnosticLightResultRow>(futures);
    }

    @Override
    protected ClosableIterator<AgnosticLightResultRow> run(FullQuery query) {
        final PalantirSqlConnection freshConn = conns.getFresh();
        final AgnosticLightResultSet results = freshConn.selectLightResultSetUnregisteredQuery(
                query.getQuery(), query.getArgs());
        return ClosableIterators.wrap(results.iterator(), new Closeable() {
            @Override
            public void close() {
                results.close();
                try {
                    freshConn.getUnderlyingConnection().close();
                } catch (SQLException e) {
                    log.error("Failed to close db connection performing reads.", e);
                }
            }
        });
    }

    private int getBatchSize() {
        return systemProperties.getCachedSystemPropertyInteger(DeclaredAtlasSystemProperty.__ATLASDB_POSTGRES_QUERY_BATCH_SIZE);
    }
}
