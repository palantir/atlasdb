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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.async.initializer.AsyncInitializer;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.CqlCapableConfigTuning;
import com.palantir.atlasdb.keyvalue.cassandra.async.queries.CqlQuerySpec;
import com.palantir.atlasdb.keyvalue.cassandra.async.queries.RowStreamAccumulator;
import com.palantir.atlasdb.keyvalue.cassandra.async.statement.preparing.CachingStatementPreparer;
import com.palantir.atlasdb.keyvalue.cassandra.async.statement.preparing.StatementPreparer;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.util.concurrent.Executor;

public final class CqlClientImpl implements CqlClient {
    private static final class InitializingWrapper extends AsyncInitializer implements AutoDelegate_CqlClient {

        private final TaggedMetricRegistry taggedMetricRegistry;
        private final int cacheSize;
        private final Cluster cluster;
        private volatile CqlClient internalImpl;

        InitializingWrapper(TaggedMetricRegistry taggedMetricRegistry, Cluster cluster, int cacheSize) {
            this.taggedMetricRegistry = taggedMetricRegistry;
            this.cluster = cluster;
            this.cacheSize = cacheSize;
        }

        @Override
        public CqlClient delegate() {
            checkInitialized();
            return internalImpl;
        }

        @Override
        protected void tryInitialize() {
            internalImpl = CqlClientImpl.create(taggedMetricRegistry, cluster.connect(), cacheSize);
        }

        @Override
        protected String getInitializingClassName() {
            return "CqlClient";
        }

        @Override
        public void close() {
            if (internalImpl != null) {
                internalImpl.close();
            }
        }
    }

    private final Session session;
    private final StatementPreparer statementPreparer;

    public static CqlClient create(
            TaggedMetricRegistry taggedMetricRegistry,
            Cluster cluster,
            CqlCapableConfigTuning tuningConfig,
            boolean initializeAsync) {
        if (initializeAsync) {
            return new InitializingWrapper(taggedMetricRegistry, cluster, tuningConfig.preparedStatementCacheSize());
        }

        return create(taggedMetricRegistry, cluster.connect(), tuningConfig.preparedStatementCacheSize());
    }

    private static CqlClient create(
            TaggedMetricRegistry taggedMetricRegistry, Session session, int preparedStatementCacheSize) {
        CachingStatementPreparer cachingStatementPreparer = CachingStatementPreparer.create(
                key -> session.prepare(key.formatQueryString()), taggedMetricRegistry, preparedStatementCacheSize);

        return new CqlClientImpl(session, cachingStatementPreparer);
    }

    private CqlClientImpl(Session session, CachingStatementPreparer statementPreparer) {
        this.session = session;
        this.statementPreparer = statementPreparer;
    }

    @Override
    public void close() {
        Cluster cluster = session.getCluster();
        session.close();
        cluster.close();
    }

    @Override
    public <V> ListenableFuture<V> executeQuery(CqlQuerySpec<V> querySpec) {
        PreparedStatement statement = statementPreparer.prepare(querySpec);
        Statement executableStatement =
                querySpec.makeExecutableStatement(statement).setConsistencyLevel(querySpec.queryConsistency());

        return execute(executableStatement, MoreExecutors.directExecutor(), querySpec.rowStreamAccumulator());
    }

    private <V> ListenableFuture<V> execute(
            Statement executableStatement, Executor executor, RowStreamAccumulator<V> rowStreamAccumulator) {
        return Futures.transformAsync(
                session.executeAsync(executableStatement), iterate(executor, rowStreamAccumulator), executor);
    }

    private <V> AsyncFunction<ResultSet, V> iterate(Executor executor, RowStreamAccumulator<V> rowStreamAccumulator) {
        return resultSet -> {
            rowStreamAccumulator.accumulateRowStream(
                    Streams.stream(resultSet).limit(resultSet.getAvailableWithoutFetching()));

            boolean wasLastPage = resultSet.getExecutionInfo().getPagingState() == null;
            if (wasLastPage) {
                return Futures.immediateFuture(rowStreamAccumulator.result());
            } else {
                ListenableFuture<ResultSet> future = resultSet.fetchMoreResults();
                return Futures.transformAsync(future, iterate(executor, rowStreamAccumulator), executor);
            }
        };
    }
}
