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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.nexus.db.sql.AgnosticLightResultRow;
import com.palantir.nexus.db.sql.AgnosticLightResultSet;

public abstract class AbstractDbReadTable implements DbReadTable {
    protected final ConnectionSupplier conns;
    protected final DbQueryFactory queryFactory;

    protected AbstractDbReadTable(ConnectionSupplier conns,
                                  DbQueryFactory queryFactory) {
        this.conns = conns;
        this.queryFactory = queryFactory;
    }

    @Override
    public ClosableIterator<AgnosticLightResultRow> getLatestRows(Iterable<byte[]> rows,
                                                                  final ColumnSelection columns,
                                                                  final long ts,
                                                                  final boolean includeValues) {
        if (columns.noColumnsSelected()) {
            return ClosableIterators.wrap(ImmutableSet.<AgnosticLightResultRow>of().iterator());
        }
        if (isSingleton(rows)) {
            return run(queryFactory.getLatestRowQuery(
                    Iterables.getOnlyElement(rows),
                    ts,
                    columns,
                    includeValues));
        }
        return getLatestRowsInternal(rows, columns, ts, includeValues);
    }

    protected abstract ClosableIterator<AgnosticLightResultRow> getLatestRowsInternal(Iterable<byte[]> rows,
                                                                                      ColumnSelection columns,
                                                                                      long ts,
                                                                                      boolean includeValues);

    @Override
    public ClosableIterator<AgnosticLightResultRow> getLatestRows(Map<byte[], Long> rows,
                                                                  final ColumnSelection columns,
                                                                  final boolean includeValues) {
        if (columns.noColumnsSelected()) {
            return ClosableIterators.wrap(ImmutableSet.<AgnosticLightResultRow>of().iterator());
        }
        if (rows.size() == 1) {
            return run(queryFactory.getLatestRowQuery(
                    Iterables.getOnlyElement(rows.keySet()),
                    Iterables.getOnlyElement(rows.values()),
                    columns,
                    includeValues));
        }
        return getLatestRowsInternal(rows, columns, includeValues);
    }

    protected abstract ClosableIterator<AgnosticLightResultRow> getLatestRowsInternal(Map<byte[], Long> rows,
                                                                                      ColumnSelection columns,
                                                                                      boolean includeValues);

    @Override
    public ClosableIterator<AgnosticLightResultRow> getAllRows(Iterable<byte[]> rows,
                                                               final ColumnSelection columns,
                                                               final long ts,
                                                               final boolean includeValues) {
        if (columns.noColumnsSelected()) {
            return ClosableIterators.wrap(ImmutableSet.<AgnosticLightResultRow>of().iterator());
        }
        if (isSingleton(rows)) {
            return run(queryFactory.getAllRowQuery(
                    Iterables.getOnlyElement(rows),
                    ts,
                    columns,
                    includeValues));
        }
        return getAllRowsInternal(rows, columns, ts, includeValues);
    }

    protected abstract ClosableIterator<AgnosticLightResultRow> getAllRowsInternal(Iterable<byte[]> rows,
                                                                                   ColumnSelection columns,
                                                                                   long ts,
                                                                                   boolean includeValues);

    @Override
    public ClosableIterator<AgnosticLightResultRow> getAllRows(Map<byte[], Long> rows,
                                                               final ColumnSelection columns,
                                                               final boolean includeValues) {
        if (columns.noColumnsSelected()) {
            return ClosableIterators.wrap(ImmutableSet.<AgnosticLightResultRow>of().iterator());
        }
        if (rows.size() == 1) {
            return run(queryFactory.getAllRowQuery(
                    Iterables.getOnlyElement(rows.keySet()),
                    Iterables.getOnlyElement(rows.values()),
                    columns,
                    includeValues));
        }
        return getAllRowsInternal(rows, columns, includeValues);
    }

    protected abstract ClosableIterator<AgnosticLightResultRow> getAllRowsInternal(Map<byte[], Long> rows,
                                                                                   ColumnSelection columns,
                                                                                   boolean includeValues);

    @Override
    public ClosableIterator<AgnosticLightResultRow> getLatestCells(Iterable<Cell> cells,
                                                                   final long ts,
                                                                   final boolean includeValue) {
        if (isSingleton(cells)) {
            return run(queryFactory.getLatestCellQuery(
                    Iterables.getOnlyElement(cells),
                    ts,
                    includeValue));
        }
        return getLatestCellsInternal(cells, ts, includeValue);
    }

    protected abstract ClosableIterator<AgnosticLightResultRow> getLatestCellsInternal(Iterable<Cell> cells,
                                                                                       long ts,
                                                                                       boolean includeValue);

    @Override
    public ClosableIterator<AgnosticLightResultRow> getLatestCells(Map<Cell, Long> cells,
                                                                   final boolean includeValue) {
        if (cells.size() == 1) {
            return run(queryFactory.getLatestCellQuery(
                    Iterables.getOnlyElement(cells.keySet()),
                    Iterables.getOnlyElement(cells.values()),
                    includeValue));
        }
        return getLatestCellsInternal(cells, includeValue);
    }

    protected abstract ClosableIterator<AgnosticLightResultRow> getLatestCellsInternal(Map<Cell, Long> cells,
                                                                                       boolean includeValue);

    @Override
    public ClosableIterator<AgnosticLightResultRow> getAllCells(Iterable<Cell> cells,
                                                                final long ts,
                                                                final boolean includeValue) {
        if (isSingleton(cells)) {
            return run(queryFactory.getAllCellQuery(
                    Iterables.getOnlyElement(cells),
                    ts,
                    includeValue));
        }
        return getAllCellsInternal(cells, ts, includeValue);
    }

    protected abstract ClosableIterator<AgnosticLightResultRow> getAllCellsInternal(Iterable<Cell> cells,
                                                                                    long ts,
                                                                                    boolean includeValue);

    @Override
    public ClosableIterator<AgnosticLightResultRow> getAllCells(Map<Cell, Long> cells,
                                                                final boolean includeValue) {
        if (cells.size() == 1) {
            return run(queryFactory.getAllCellQuery(
                    Iterables.getOnlyElement(cells.keySet()),
                    Iterables.getOnlyElement(cells.values()),
                    includeValue));
        }
        return getAllCellsInternal(cells, includeValue);
    }

    protected abstract ClosableIterator<AgnosticLightResultRow> getAllCellsInternal(Map<Cell, Long> cells,
                                                                                    boolean includeValue);

    @Override
    public ClosableIterator<AgnosticLightResultRow> getRange(RangeRequest range, long ts, int maxRows) {
        FullQuery query = queryFactory.getRangeQuery(range, ts, maxRows);
        AgnosticLightResultSet results = conns.get().selectLightResultSetUnregisteredQuery(
                query.getQuery(), query.getArgs());
        results.setFetchSize(maxRows);
        return ClosableIterators.wrap(results.iterator(), results);
    }

    @Override
    public boolean hasOverflowValues() {
        return queryFactory.hasOverflowValues();
    }

    @Override
    public ClosableIterator<AgnosticLightResultRow> getOverflow(Collection<OverflowValue> overflowIds) {
        Collection<FullQuery> queries = queryFactory.getOverflowQueries(overflowIds);
        if (queries.size() == 1) {
            return run(Iterables.getOnlyElement(queries));
        }
        Queue<Future<ClosableIterator<AgnosticLightResultRow>>> futures = Queues.newArrayDeque();
        for (final FullQuery query : queries) {
            futures.add(submit(MoreExecutors.directExecutor(), query));
        }
        return new LazyClosableIterator<AgnosticLightResultRow>(futures);
    }

    private boolean isSingleton(Iterable<?> iterable) {
        Iterator<?> iter = iterable.iterator();
        if (!iter.hasNext()) {
            return false;
        }
        iter.next();
        return !iter.hasNext();
    }

    protected Callable<ClosableIterator<AgnosticLightResultRow>> getCallable(final FullQuery query) {
        return new Callable<ClosableIterator<AgnosticLightResultRow>>() {
            @Override
            public ClosableIterator<AgnosticLightResultRow> call() throws InterruptedException {
                return run(query);
            }
        };
    }

    protected Future<ClosableIterator<AgnosticLightResultRow>> submit(Executor exec, FullQuery query) {
        FutureTask<ClosableIterator<AgnosticLightResultRow>> task =
                new FutureClosableIteratorTask<AgnosticLightResultRow>(getCallable(query));
        exec.execute(task);
        return task;
    }

    protected abstract ClosableIterator<AgnosticLightResultRow> run(FullQuery query);
}
