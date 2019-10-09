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

package com.palantir.atlasdb.keyvalue.cassandra.async;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.async.initializer.AsyncInitializer;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.CqlCapableConfigTuning;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public final class CqlClientImpl implements CqlClient {

    private static final class InitializingWrapper extends AsyncInitializer implements AutoDelegate_CqlClient {

        private final TaggedMetricRegistry taggedMetricRegistry;
        private final int cacheSize;
        private final Cluster cluster;
        private final ExecutorService executorService;
        private volatile CqlClient internalImpl;

        InitializingWrapper(
                TaggedMetricRegistry taggedMetricRegistry,
                Cluster cluster,
                ExecutorService executorService,
                int cacheSize) {
            this.taggedMetricRegistry = taggedMetricRegistry;
            this.cluster = cluster;
            this.executorService = executorService;
            this.cacheSize = cacheSize;
        }

        @Override
        public CqlClient delegate() {
            checkInitialized();
            return internalImpl;
        }

        @Override
        public CqlQueryBuilder asyncQueryBuilder() {
            return internalImpl.asyncQueryBuilder();
        }

        @Override
        public <R> ListenableFuture<R> execute(Executable<R> executable) {
            return internalImpl.execute(executable);
        }

        @Override
        protected void tryInitialize() {
            internalImpl = CqlClientImpl.create(taggedMetricRegistry, cluster.connect(), executorService, cacheSize);
        }

        @Override
        protected String getInitializingClassName() {
            return "CqlClient";
        }

        @Override
        public void close() throws Exception {
            if (internalImpl != null) {
                internalImpl.close();
            }
        }
    }

    private static final Logger log = LoggerFactory.getLogger(CqlClientFactoryImpl.class);

    private final Session session;
    private final ExecutorService executorService;
    private final StatementPreparer statementPreparer;

    public static CqlClient create(TaggedMetricRegistry taggedMetricRegistry, Cluster cluster,
            ExecutorService executorService, CqlCapableConfigTuning tuningConfig, boolean initializeAsync) {
        if (initializeAsync) {
            return new InitializingWrapper(
                    taggedMetricRegistry,
                    cluster,
                    executorService,
                    tuningConfig.preparedStatementCacheSize());
        }

        return create(
                taggedMetricRegistry,
                cluster.connect(),
                executorService,
                tuningConfig.preparedStatementCacheSize());
    }

    private static CqlClient create(
            TaggedMetricRegistry taggedMetricRegistry,
            Session session,
            ExecutorService executorService,
            int preparedStatementCacheSize) {
        QueryCache queryCache = QueryCache.create(
                key -> session.prepare(key.formatQueryString()),
                taggedMetricRegistry,
                preparedStatementCacheSize);

        return new CqlClientImpl(session, executorService, queryCache);
    }

    private CqlClientImpl(Session session, ExecutorService executorService, QueryCache statementPreparer) {
        this.session = session;
        this.executorService = executorService;
        this.statementPreparer = statementPreparer;
    }

    @Override
    public void close() {
        try {
            executorService.shutdown();
            if (executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                log.info("CqlClient executor service terminated properly.");
            } else {
                log.warn("CqlClient executor service timed out before shutting down, shutting down forcefully");
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.warn("Thread was interrupted while waiting for CqlClient to terminate.", e);
        } catch (Exception e) {
            log.warn("CqlClient exception on executor service termination", e);
        }
        Cluster cluster = session.getCluster();
        session.close();
        cluster.close();
    }

    @Override
    public CqlQueryBuilder asyncQueryBuilder() {
        return new CqlQueryBuilderImpl();
    }

    @Override
    public <R> ListenableFuture<R> execute(Executable<R> executable) {
        return executable.execute(executorService);
    }

    private class CqlQueryImpl<R> implements CqlQuery<R> {

        private final RowStreamAccumulator<R> rowStreamAccumulator;
        private final BoundStatement boundStatement;

        CqlQueryImpl(BoundStatement boundStatement, RowStreamAccumulator<R> rowStreamAccumulator) {
            this.boundStatement = boundStatement;
            this.rowStreamAccumulator = rowStreamAccumulator;
        }

        /**
         * This method is implemented to process only the currently available data page. After each page is processed we
         * asynchronously request more data and process it. That way no thread is blocked waiting to retrieve the next
         * page.
         *
         * @return {@code AsyncFunction} which will transform the {@code Future} containing the {@code resultSet}
         */
        private AsyncFunction<ResultSet, R> iterate(Executor executor) {
            return resultSet -> {
                rowStreamAccumulator.accumulateRowStream(Streams.stream(resultSet)
                        .limit(resultSet.getAvailableWithoutFetching()));

                boolean wasLastPage = resultSet.getExecutionInfo().getPagingState() == null;
                if (wasLastPage) {
                    return Futures.immediateFuture(rowStreamAccumulator.result());
                } else {
                    ListenableFuture<ResultSet> future = resultSet.fetchMoreResults();
                    return Futures.transformAsync(future, iterate(executor), executor);
                }
            };
        }

        @Override
        public ListenableFuture<R> execute(Executor executor) {
            return Futures.transformAsync(
                    session.executeAsync(boundStatement),
                    iterate(executor),
                    executor);
        }
    }

    private class CqlQueryBuilderImpl implements CqlQueryBuilder {

        @Override
        public <R> CqlQuery<R> build(CqlQuerySpec<R> querySpec) {
            PreparedStatement statement = statementPreparer.prepare(querySpec);

            BoundStatement boundStatement = querySpec.bind(statement.bind());

            return new CqlQueryImpl<>(boundStatement, querySpec.rowStreamAccumulatorFactory().get());
        }
    }
}
