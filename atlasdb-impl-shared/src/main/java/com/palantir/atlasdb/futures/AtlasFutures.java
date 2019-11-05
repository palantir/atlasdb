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

package com.palantir.atlasdb.futures;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.common.base.Throwables;
import com.palantir.common.streams.KeyedStream;

import net.javacrumbs.futureconverter.java8guava.FutureConverter;

public final class AtlasFutures {
    private AtlasFutures() {

    }

    public static <T, R> ListenableFuture<Map<T, R>> allAsMap(
            Map<T, ListenableFuture<Optional<R>>> inputToListenableFutureMap,
            ExecutorService executorService) {
        return Futures.whenAllSucceed(inputToListenableFutureMap.values())
                .call(() -> KeyedStream.stream(inputToListenableFutureMap)
                                .map(AtlasFutures::getDone)
                                .filter(Optional::isPresent)
                                .map(Optional::get)
                                .collectToMap(),
                        executorService);
    }

    public static <R> R getDone(ListenableFuture<R> resultFuture) {
        try {
            return Futures.getDone(resultFuture);
        } catch (ExecutionException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e.getCause());
        }
    }

    public static <R> R getUnchecked(ListenableFuture<R> listenableFuture) {
        try {
            return listenableFuture.get();
        } catch (ExecutionException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e.getCause());
        } catch (Exception e) {
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

    public static <R> ListenableFuture<R> toListenableFuture(CompletableFuture<R> completableFuture) {
        return FutureConverter.toListenableFuture(completableFuture);
    }

    public static <V> ListenableFuture<V> toListenableFuture(CompletionStage<V> completionStage) {
        return toListenableFuture(completionStage.toCompletableFuture());
    }
}
