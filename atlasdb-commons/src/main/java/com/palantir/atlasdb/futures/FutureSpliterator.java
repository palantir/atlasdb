/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Consumer;

public class FutureSpliterator<T> implements Spliterator<T> {
    private final Future<Spliterator<T>> future;

    public FutureSpliterator(Future<Spliterator<T>> future) {
        this.future = future;
    }

    @Override
    public long estimateSize() {
        if (future.isDone()) {
            try {
                return future.get().estimateSize();
            } catch (Exception e) {
                // pass
            }
        }
        return Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
        return Spliterator.ORDERED;
    }

    @Override
    public boolean tryAdvance(Consumer<? super T> consumer) {
        return AtlasFutures.getUnchecked(future).tryAdvance(consumer);
    }

    @Override
    public void forEachRemaining(Consumer<? super T> consumer) {
        AtlasFutures.getUnchecked(future).forEachRemaining(consumer);
    }

    @Override
    public Spliterator<T> trySplit() {
        return null;
    }

    public static <K, V> FutureSpliterator<Map.Entry<K, V>> ofEntries(ListenableFuture<Map<K, V>> futureMap) {
        return new FutureSpliterator<>(
                Futures.transform(futureMap, map -> map.entrySet().spliterator(), MoreExecutors.directExecutor()));
    }

    public static <K, V> FutureSpliterator<Map.Entry<K, V>> ofEntries(CompletableFuture<Map<K, V>> futureMap) {
        return new FutureSpliterator<>(futureMap.thenApply(Map::entrySet).thenApply(Set::spliterator));
    }
}
