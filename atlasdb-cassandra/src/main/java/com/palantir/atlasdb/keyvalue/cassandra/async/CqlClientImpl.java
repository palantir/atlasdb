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

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.AddressTranslator;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.logsafe.Preconditions;
import com.palantir.tracing.AsyncTracer;

public final class CqlClientImpl implements CqlClient {

    static class SimpleAddressTranslator implements AddressTranslator {

        private final Map<String, InetSocketAddress> mapper;

        SimpleAddressTranslator(CassandraKeyValueServiceConfig config) {
            this.mapper = config.addressTranslation();
        }

        @Override
        public void init(Cluster cluster) {
        }

        @Override
        public InetSocketAddress translate(InetSocketAddress address) {
            InetSocketAddress temp = mapper.getOrDefault(address.getHostString(), address);
            return new InetSocketAddress(temp.getAddress(), temp.getPort());
        }

        @Override
        public void close() {
        }
    }

    private final Session session;
    private final String sessionName;
    private final Executor executor;

    public static CqlClient create(String clientName, Session session,
            Executor executor) {
        return new CqlClientImpl(clientName, session, executor);
    }

    private CqlClientImpl(String sessionName, Session session, Executor executor) {
        this.sessionName = sessionName;
        this.session = session;
        this.executor = executor;
    }


    @Override
    public String sessionName() {
        return sessionName;
    }

    @Override
    public void close() {
        session.close();
    }

    // TODO (OStevan): make a prepared statement cache as per
    //  https://docs.datastax.com/en/developer/java-driver/3.7/manual/statements/prepared/
    @Override
    public PreparedStatement prepareStatement(String queryString) {
        return session.prepare(queryString);
    }

    @Override
    public <V, R> ListenableFuture<R> executeQuery(CqlQuery<V, R> cqlQuery) {
        Visitor<V, R> visitor = cqlQuery.createVisitor();
        return transform(Futures.transformAsync(session.executeAsync(cqlQuery.boundStatement()), iterate(visitor),
                executor), Visitor::result);
    }

    @Override
    public <V, P, R> ListenableFuture<R> executeQueries(Stream<CqlQuery<V, P>> inputStatementPairStream,
            Function<List<P>, R> transformer) {
        List<ListenableFuture<P>> allResults = inputStatementPairStream
                .map(this::executeQuery)
                .collect(Collectors.toList());

        return transform(Futures.allAsList(allResults), transformer);
    }

    private <I, O> ListenableFuture<O> transform(ListenableFuture<I> input,
            Function<? super I, ? extends O> function) {
        AsyncTracer asyncTracer = new AsyncTracer();
        return Futures.transform(input, p -> asyncTracer.withTrace(() -> function.apply(p)),
                executor);
    }


    private <V, R> AsyncFunction<ResultSet, Visitor<V, R>> iterate(
            final Visitor<V, R> visitor) {
        return rs -> {
            Preconditions.checkArgument(rs != null, "ResultSet should not be null when iterating");
            int remainingInPage = rs.getAvailableWithoutFetching();

            visitor.visitResultSet(rs, remainingInPage);

            boolean wasLastPage = rs.getExecutionInfo().getPagingState() == null;
            if (wasLastPage) {
                return Futures.immediateFuture(visitor);
            } else {
                ListenableFuture<ResultSet> future = rs.fetchMoreResults();
                return Futures.transformAsync(future, iterate(visitor), executor);
            }
        };
    }
}
