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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.common.streams.KeyedStream;

/**
 * Default implementation of {@link CqlFuturesCombiner}. This implementation combines the futures on a passed
 * {@link ExecutorService}. Also it takes ownership of the passed {@link ExecutorService}.
 */
public class DefaultCqlFuturesCombiner implements CqlFuturesCombiner {
    private final ExecutorService executorService;

    public DefaultCqlFuturesCombiner(ExecutorService executorService) {
        this.executorService = executorService;
    }

    @Override
    public <T, R> ListenableFuture<Map<T, R>> allAsMap(
            Map<T, ListenableFuture<Optional<R>>> inputToListenableFutureMap) {
        return Futures.whenAllSucceed(inputToListenableFutureMap.values())
                .call(() -> KeyedStream.stream(inputToListenableFutureMap)
                                .map(DefaultCqlFuturesCombiner::getDone)
                                .filter(Optional::isPresent)
                                .map(Optional::get)
                                .collectToMap(),
                        executorService);
    }

    private static <V> V getDone(Future<V> listenableFuture) {
        try {
            return Futures.getDone(listenableFuture);
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }

    @Override
    public void close() {
        executorService.shutdown();
    }
}
