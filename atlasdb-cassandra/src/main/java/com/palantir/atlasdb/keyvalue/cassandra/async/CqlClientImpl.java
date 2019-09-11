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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.logsafe.Preconditions;

public final class CqlClientImpl implements CqlClient {

    private final Session session;
    private final Executor executor;

    public static CqlClient create(Session session,
            Executor executor) {
        return new CqlClientImpl(session, executor);
    }

    private CqlClientImpl(Session session, Executor executor) {
        this.session = session;
        this.executor = executor;
    }

    @Override
    public void close() {
        Cluster cluster = session.getCluster();
        session.close();
        cluster.close();
    }

    @Override
    public CqlQueryBuilder<Object> queryBuilder() {
        return new CqlQueryBuilderImpl<>();
    }

    // TODO (OStevan): make a prepared statement cache as per
    //  https://docs.datastax.com/en/developer/java-driver/3.7/manual/statements/prepared/
    private PreparedStatement prepareStatement(String queryString) {
        return session.prepare(queryString);
    }

    private class CqlQueryImpl<R> implements CqlQuery<R> {

        private final RowStreamAccumulator<R> rowStreamAccumulator;
        private final BoundStatement boundStatement;

        CqlQueryImpl(BoundStatement boundStatement, RowStreamAccumulator<R> rowStreamAccumulator) {
            this.boundStatement = boundStatement;
            this.rowStreamAccumulator = rowStreamAccumulator;
        }

        final AsyncFunction<ResultSet, R> iterate() {
            return resultSet -> {
                Preconditions.checkArgument(resultSet != null, "ResultSet should not be null when iterating");

                rowStreamAccumulator.accumulateRowStream(Streams.stream(resultSet)
                        .limit(resultSet.getAvailableWithoutFetching()));

                boolean wasLastPage = resultSet.getExecutionInfo().getPagingState() == null;
                if (wasLastPage) {
                    return Futures.immediateFuture(rowStreamAccumulator.result());
                } else {
                    ListenableFuture<ResultSet> future = resultSet.fetchMoreResults();
                    return Futures.transformAsync(future, iterate(), executor);
                }
            };
        }

        @Override
        public ListenableFuture<R> execute() {
            return Futures.transformAsync(session.executeAsync(boundStatement), iterate(), executor);
        }
    }

    private class CqlQueryBuilderImpl<R> implements CqlQueryBuilder<R> {

        private String queryString;
        private Map<String, Object> args = new HashMap<>();
        private RowStreamAccumulator<R> rowStreamAccumulator;

        @Override
        public CqlQueryBuilder<R> setQueryString(String query) {
            this.queryString = query;
            return this;
        }

        @Override
        public CqlQueryBuilder<R> setArg(String argumentName, Object argument) {
            args.put(argumentName, argument);
            return this;
        }

        @Override
        public <T> CqlQueryBuilder<T> setResultSetVisitor(RowStreamAccumulator<T> visitor) {
            this.rowStreamAccumulator = (RowStreamAccumulator<R>) visitor;
            return (CqlQueryBuilder<T>) this;
        }

        @Override
        public CqlQuery<R> build() {
            Preconditions.checkNotNull(queryString, "Empty query string");
            PreparedStatement statement = CqlClientImpl.this.prepareStatement(queryString);

            Object[] argsArray = new Object[args.size()];
            for (int i = 0; i < args.size(); i++) {
                Preconditions.checkState(this.args.keySet().contains(statement.getVariables().getName(i)),
                        "Set argument is not expected");
                argsArray[i] = args.get(statement.getVariables().getName(i));
            }

            BoundStatement boundStatement = statement.bind(argsArray);

            Preconditions.checkNotNull(rowStreamAccumulator, "RowStreamAccumulator is not set");
            return new CqlQueryImpl<>(boundStatement, rowStreamAccumulator);
        }
    }
}
