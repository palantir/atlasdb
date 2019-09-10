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

import java.io.Closeable;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.util.concurrent.ListenableFuture;

public interface CqlClusterClient extends Closeable {

    interface CqlQuery<V, R> {
        BoundStatement boundStatement();

        Visitor<V, R> createVisitor();
    }

    interface Visitor<V, R> {

        void visit(V value);

        R result();


        V retrieveRow(Row row);

        default void visitResultSet(ResultSet resultSet, int numberOfRowsToVisit) {
            int remaining = numberOfRowsToVisit;
            if (remaining <= 0) {
                return;
            }
            for (Row row : resultSet) {
                visit(retrieveRow(row));
                if (--remaining == 0) {
                    return;
                }
            }
        }
    }

    String sessionName();

    CqlClusterClient start();

    void close();

    PreparedStatement prepareStatement(String queryString);

    <V, R> ListenableFuture<R> executeQuery(CqlQuery<V, R> cqlQuery);

    <V, P, R> ListenableFuture<R> executeQueries(Stream<CqlQuery<V, P>> boundStatementStream,
            Function<List<P>, R> transformer);
}
