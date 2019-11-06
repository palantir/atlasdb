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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Supplier;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.async.initializer.AsyncInitializer;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.CqlCapableConfigTuning;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.keyvalue.cassandra.async.queries.CqlQuerySpec;
import com.palantir.atlasdb.keyvalue.cassandra.async.queries.RowAccumulator;
import com.palantir.atlasdb.keyvalue.cassandra.async.statement.preparing.CachingStatementPreparer;
import com.palantir.atlasdb.keyvalue.cassandra.async.statement.preparing.StatementPreparer;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public final class CqlClientImpl implements CqlClient {
    private static final class InitializingWrapper extends AsyncInitializer implements AutoDelegate_CqlClient {

        private final TaggedMetricRegistry taggedMetricRegistry;
        private final int cacheSize;
        private final Supplier<CqlSession> cqlSessionSupplier;
        private volatile CqlClient internalImpl;

        InitializingWrapper(
                TaggedMetricRegistry taggedMetricRegistry,
                Supplier<CqlSession> cqlSessionSupplier,
                int cacheSize) {
            this.taggedMetricRegistry = taggedMetricRegistry;
            this.cqlSessionSupplier = cqlSessionSupplier;
            this.cacheSize = cacheSize;
        }

        @Override
        public CqlClient delegate() {
            checkInitialized();
            return internalImpl;
        }

        @Override
        protected void tryInitialize() {
            internalImpl = CqlClientImpl.create(taggedMetricRegistry, cqlSessionSupplier, cacheSize);
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

    private final CqlSession session;
    private final StatementPreparer statementPreparer;

    public static CqlClient create(
            TaggedMetricRegistry taggedMetricRegistry,
            Supplier<CqlSession> sessionSupplier,
            CqlCapableConfigTuning tuningConfig,
            boolean initializeAsync) {
        if (initializeAsync) {
            return new InitializingWrapper(
                    taggedMetricRegistry,
                    sessionSupplier,
                    tuningConfig.preparedStatementCacheSize());
        }

        return create(
                taggedMetricRegistry,
                sessionSupplier,
                tuningConfig.preparedStatementCacheSize());
    }

    private static CqlClient create(
            TaggedMetricRegistry taggedMetricRegistry,
            Supplier<CqlSession> cqlSessionSupplier,
            int preparedStatementCacheSize) {
        CqlSession cqlSession = cqlSessionSupplier.get();
        CachingStatementPreparer cachingStatementPreparer = CachingStatementPreparer.create(
                key -> cqlSession.prepare(key.formatQueryString()),
                taggedMetricRegistry,
                preparedStatementCacheSize);

        return new CqlClientImpl(cqlSession, cachingStatementPreparer);
    }

    @VisibleForTesting
    CqlClientImpl(CqlSession session, StatementPreparer statementPreparer) {
        this.session = session;
        this.statementPreparer = statementPreparer;
    }

    @Override
    public void close() {
        session.close();
    }

    @Override
    public <V> ListenableFuture<V> executeQuery(CqlQuerySpec<V> querySpec) {
        PreparedStatement statement = statementPreparer.prepare(querySpec);
        Statement executableStatement = querySpec.makeExecutableStatement(statement)
                .setConsistencyLevel(querySpec.queryConsistency());

        return execute(executableStatement, querySpec.rowAccumulator());
    }

    private <V> ListenableFuture<V> execute(Statement statement, RowAccumulator<V> rowAccumulator) {
        CompletionStage<AsyncResultSet> futureRs = session.executeAsync(statement);
        return AtlasFutures.toListenableFuture(futureRs.thenCompose(processRows(rowAccumulator)));
    }

    private <V> Function<AsyncResultSet, CompletionStage<V>> processRows(RowAccumulator<V> rowAccumulator) {
        return asyncResultSet -> {
            rowAccumulator.accumulateRows(asyncResultSet.currentPage());

            if (!asyncResultSet.hasMorePages()) {
                return CompletableFuture.completedFuture(rowAccumulator.result());
            }
            return asyncResultSet.fetchNextPage().thenCompose(processRows(rowAccumulator));
        };
    }
}
