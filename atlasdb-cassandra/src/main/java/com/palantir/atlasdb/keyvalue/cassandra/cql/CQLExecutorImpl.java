/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra.cql;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;
import com.google.common.primitives.UnsignedBytes;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServices;
import com.palantir.atlasdb.keyvalue.cassandra.ValueExtractor;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.base.Throwables;
import com.palantir.common.visitor.Visitor;
import com.palantir.tracing.AsyncTracer;

public final class CQLExecutorImpl implements CQLExecutor {

    private static final Logger log = LoggerFactory.getLogger(CQLExecutorImpl.class);

    private final MetricsManager metricsManager;
    private final CassandraKeyValueServiceConfig config;
    private final SafeKeyspace keyspace;
    private final CQLQueries cqlQueries;
    private final CqlFieldNameProvider fieldNameProvider;
    private final ConsistencyLevel readConsistency = ConsistencyLevel.LOCAL_QUORUM;
    private final CQLSessionFactory.ClusterSession session;
    private final Timer getTimer;

    public CQLExecutorImpl(
            MetricsManager metricsManager,
            CassandraKeyValueServiceConfig config,
            CQLSessionFactory.ClusterSession session,
            CqlFieldNameProvider fieldNameProvider) {
        this.metricsManager = metricsManager;
        this.config = config;
        this.keyspace = SafeKeyspace.of(config.getKeyspaceOrThrow());
        this.cqlQueries = new CQLQueries(keyspace);
        this.session = session;
        this.fieldNameProvider = fieldNameProvider;
        getTimer = metricsManager.registerOrGetTimer(CQLExecutor.class, "get");
    }

    @Override
    public ListenableFuture<Map<Cell, Value>> get(TableReference tableRef, Map<Cell, Long> timestampByCell) {

        if (timestampByCell.isEmpty()) {
            log.info("Attempted get on '{}' table with empty cells", tableRef);
            return Futures.immediateFuture(ImmutableMap.of());
        }

        try {
            Timer.Context time = getTimer.time();
            long firstTs = timestampByCell.values().iterator().next();
            if (Iterables.all(timestampByCell.values(), Predicates.equalTo(firstTs))) {
                return transform(getAsync("get", tableRef, timestampByCell.keySet(), firstTs), ret -> {
                    time.close();
                    return ret;
                });
            }

            SetMultimap<Long, Cell> cellsByTs = Multimaps.invertFrom(
                    Multimaps.forMap(timestampByCell), HashMultimap.create());
            List<ListenableFuture<Map<Cell, Value>>> allResults = Lists.newArrayListWithCapacity(
                    cellsByTs.keySet().size());
            for (long ts : cellsByTs.keySet()) {
                allResults.add(getAsync("get", tableRef, cellsByTs.get(ts), ts));
            }
            // TODO(jakubk): Use a single visitor.
            return transform(Futures.allAsList(allResults), results -> {
                ImmutableMap.Builder<Cell, Value> builder = ImmutableMap.builder();
                results.forEach(builder::putAll);
                time.close();
                return builder.build();
            });
        } catch (Exception e) {
            return Futures.immediateFailedFuture(Throwables.unwrapAndReturnAtlasDbDependencyException(e));
        }
    }

    private ListenableFuture<Map<Cell, Value>> getAsync(
            String kvsMethodName,
            TableReference tableRef,
            Set<Cell> cells,
            long maxTimestampExclusive) {
        StartTsResultsCollector collector = new StartTsResultsCollector(maxTimestampExclusive);
        ListenableFuture<?> get = loadWithTsAsync(
                kvsMethodName,
                tableRef,
                cells,
                maxTimestampExclusive,
                false,
                collector,
                readConsistency);
        return transform(get, $ -> collector.collectedResults);
    }

    private ListenableFuture<?> loadWithTsAsync(
            String kvsMethodName,
            final TableReference tableRef,
            final Set<Cell> cells,
            final long startTs,
            boolean loadAllTs,
            final Visitor<Multimap<Cell, Value>> visitor,
            final ConsistencyLevel consistency) {
        List<ListenableFuture<?>> queries = Lists.newArrayListWithCapacity(cells.size());
        SortedSetMultimap<byte[], Cell> cellsByCol =
                TreeMultimap.create(UnsignedBytes.lexicographicalComparator(), Ordering.natural());
        for (Cell cell : cells) {
            cellsByCol.put(cell.getColumnName(), cell);
        }

        // TODO(jakubk): Should still partition by host somewhere here later
        // TODO(jakubk): And then do the smart batching to not overwhelm them.

        // https://docs.datastax.com/en/developer/java-driver/3.6/manual/statements/built/

        // CellLoader.translatePartitionToKeyPredicates

        // CassandraTableCreator#createTable

        for (Map.Entry<byte[], SortedSet<Cell>> entry : Multimaps.asMap(cellsByCol).entrySet()) {
            for (List<Cell> batch : Iterables.partition(entry.getValue(), config.fetchBatchCount())) {
                Statement select = cqlQueries.select(tableRef, entry.getKey(), batch, startTs, loadAllTs, consistency);

                // TODO(jakubk): Use prepared queries
                queries.add(collectResults(session.session().executeAsync(select), visitor, loadAllTs));
            }
        }

        return Futures.whenAllSucceed(queries).call(() -> null, MoreExecutors.directExecutor());
    }

    private ListenableFuture<?> collectResults(
            ResultSetFuture resultSetFuture,
            Visitor<Multimap<Cell, Value>> visitor,
            boolean loadAllTs) {
        return transformAsync(resultSetFuture, iterate(1, visitor, loadAllTs));
    }

    private AsyncFunction<ResultSet, Void> iterate(
            final int page,
            Visitor<Multimap<Cell, Value>> visitor,
            boolean loadAllTs) {
        return rs -> {

            // How far we can go without triggering the blocking fetch:
            int remainingInPage = rs.getAvailableWithoutFetching();
            if (remainingInPage == 0) {
                return Futures.immediateFuture(null);
            }

            visitResults(rs, remainingInPage, visitor, loadAllTs);

            boolean wasLastPage = rs.getExecutionInfo().getPagingState() == null;
            if (wasLastPage) {
                return Futures.immediateFuture(null);
            } else {
                ListenableFuture<ResultSet> future = rs.fetchMoreResults();
                return transformAsync(future, iterate(page + 1, visitor, loadAllTs));
            }
        };
    }

    private void visitResults(
            ResultSet rs,
            int remainingInPage,
            Visitor<Multimap<Cell, Value>> visitor,
            /*String query,*/
            boolean loadAllTs) {

        Multimap<Cell, Value> res;
        if (loadAllTs) {
            res = HashMultimap.create();
        } else {
            res = HashMultimap.create(remainingInPage, 1);
        }

        for (Row row : rs) {
            res.put(Cell.create(getRowName(row), getColName(row)),
                    Value.create(getValue(row), getTs(row)));
            if (--remainingInPage == 0) {
                break;
            }
        }

        //        cqlKeyValueServices.logTracedQuery(query, resultSet, session, cqlStatementCache.normalQuery);
        visitor.visit(res);
    }

    interface ThreadSafeCQLResultVisitor extends Visitor<Multimap<Cell, Value>> {
        // marker
    }

    class StartTsResultsCollector implements ThreadSafeCQLResultVisitor {
        final Map<Cell, Value> collectedResults = Maps.newConcurrentMap();
        final ValueExtractor extractor = new ValueExtractor(metricsManager, collectedResults);
        final long startTs;

        StartTsResultsCollector(long startTs) {
            this.startTs = startTs;
        }

        @Override
        public void visit(Multimap<Cell, Value> results) {
            for (Map.Entry<Cell, Value> e : results.entries()) {
                if (results.get(e.getKey()).size() > 1) {
                    throw new IllegalStateException("Too many results retrieved for cell " + e.getKey());
                }
                collectedResults.put(e.getKey(), e.getValue());
            }
        }
    }

    private <I, O> ListenableFuture<O> transform(ListenableFuture<I> input, Function<? super I, ? extends O> function) {
        AsyncTracer asyncTracer = new AsyncTracer();
        return Futures.transform(input, i -> asyncTracer.withTrace(() -> function.apply(i)),
                session.callbackExecutor());
    }

    private <I, O> ListenableFuture<O> transformAsync(
            ListenableFuture<I> inputFuture,
            AsyncFunction<I, O> function) {
        AsyncTracer asyncTracer = new AsyncTracer();
        return Futures.transformAsync(
                inputFuture,
                value -> asyncTracer.withTrace(() -> function.apply(value)),
                session.callbackExecutor());
    }

    private byte[] getRowName(Row row) {
        return CassandraKeyValueServices.getBytesFromByteBuffer(row.getBytes(fieldNameProvider.row()));
    }

    private byte[] getColName(Row row) {
        return CassandraKeyValueServices.getBytesFromByteBuffer(row.getBytes(fieldNameProvider.column()));
    }

    private long getTs(Row row) {
        return ~row.getLong(fieldNameProvider.timestamp());
    }

    private byte[] getValue(Row row) {
        return CassandraKeyValueServices.getBytesFromByteBuffer(row.getBytes(fieldNameProvider.value()));
    }
}
