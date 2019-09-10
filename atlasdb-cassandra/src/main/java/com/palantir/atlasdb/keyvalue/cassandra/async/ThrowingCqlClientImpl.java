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

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import com.datastax.driver.core.PreparedStatement;
import com.google.common.util.concurrent.ListenableFuture;

public class ThrowingCqlClientImpl implements CqlClient {
    @Override
    public String sessionName() {
        return "throwing-cql-client";
    }

    @Override
    public void close() {

    }

    @Override
    public PreparedStatement prepareStatement(String queryString) {
        throw new UnsupportedOperationException("Not configured to use CQL client, check your KVS config file.");
    }

    @Override
    public <V, R> ListenableFuture<R> executeQuery(CqlQuery<V, R> cqlQuery) {
        throw new UnsupportedOperationException("Not configured to use CQL client, check your KVS config file.");
    }

    @Override
    public <V, P, R> ListenableFuture<R> executeQueries(Stream<CqlQuery<V, P>> boundStatementStream,
            Function<List<P>, R> transformer) {
        throw new UnsupportedOperationException("Not configured to use CQL client, check your KVS config file.");
    }
}
