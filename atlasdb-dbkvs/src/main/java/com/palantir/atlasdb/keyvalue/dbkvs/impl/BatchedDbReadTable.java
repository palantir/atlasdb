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

import java.io.Closeable;
import java.sql.Connection;
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
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.dbkvs.DbSharedConfig;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.common.base.Throwables;
import com.palantir.nexus.db.sql.AgnosticLightResultRow;
import com.palantir.nexus.db.sql.AgnosticLightResultSet;
import com.palantir.nexus.db.sql.SqlConnection;

public class BatchedDbReadTable extends AbstractDbReadTable {
    private static final Logger log = LoggerFactory.getLogger(BatchedDbReadTable.class);
    private final Executor exec;
    private final DbSharedConfig config;

    protected BatchedDbReadTable(ConnectionSupplier conns,
                                 DbQueryFactory queryFactory,
                                 Executor exec,
                                 DbSharedConfig config) {
        super(conns, queryFactory);
        this.exec = exec;
        this.config = config;
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
        final SqlConnection freshConn = conns.getFresh();
        try {
            final AgnosticLightResultSet results = freshConn.selectLightResultSetUnregisteredQuery(
                    query.getQuery(), query.getArgs());
            return ClosableIterators.wrap(results.iterator(), new Closeable() {
                @Override
                public void close() {
                    results.close();
                    closeConnection(freshConn.getUnderlyingConnection());
                }
            });
        } catch (Exception e) {
            closeConnection(freshConn.getUnderlyingConnection());
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

    private void closeConnection(Connection conn) {
        try {
            conn.close();
        } catch (SQLException e) {
            log.error("Failed to close db connection performing reads.", e);
        }
    }

    private int getBatchSize() {
        return config.fetchBatchSize();
    }
}
