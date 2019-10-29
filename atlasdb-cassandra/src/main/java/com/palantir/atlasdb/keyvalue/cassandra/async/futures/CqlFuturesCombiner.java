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

package com.palantir.atlasdb.keyvalue.cassandra.async.futures;

import java.util.Map;
import java.util.Optional;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * Declares an interface for combining futures which represent results of queries.
 */
public interface CqlFuturesCombiner extends AutoCloseable {
    /**
     * Creates a new {@code ListenableFuture} whose value is a map containing the values of all its
     * input futures, if all succeed. Input key-value pairs for which the input futures resolve to
     * {@link Optional#empty()} are filtered out.
     *
     * @param inputToListenableFutureMap query input to {@link ListenableFuture} of the query result
     * @param <T> type of query input
     * @param <R> type of query result
     * @return {@link ListenableFuture} of the combined map
     */
    <T, R> ListenableFuture<Map<T, R>> allAsMap(Map<T, ListenableFuture<Optional<R>>> inputToListenableFutureMap);

    @Override
    void close();
}
