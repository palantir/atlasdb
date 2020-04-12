/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.v2.api.util;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;

public final class Autobatcher<I, O> implements AsyncFunction<I, O>, Runnable {
    private final AsyncFunction<Set<I>, Map<I, O>> backingFunction;
    private final Executor executor;
    private final Map<I, SettableFuture<O>> futures = new LinkedHashMap<>();
    private boolean isScheduled = false;

    public Autobatcher(AsyncFunction<Set<I>, Map<I, O>> backingFunction,
            Executor executor) {
        this.backingFunction = backingFunction;
        this.executor = executor;
    }

    @Override
    public ListenableFuture<O> apply(I input) {
        ListenableFuture<O> future = futures.computeIfAbsent(input, $ -> SettableFuture.create());
        scheduleIfNecessary();
        return future;
    }

    private void scheduleIfNecessary() {
        if (isScheduled) {
            return;
        }
        isScheduled = true;
        executor.execute(this);
    }

    @Override
    public void run() {
        Map<I, SettableFuture<O>> futuresCopy = ImmutableMap.copyOf(futures);
        futures.clear();
        isScheduled = false;
        ListenableFuture<Map<I, O>> result;
        try {
            result = backingFunction.apply(futuresCopy.keySet());
        } catch (Exception e) {
            markAllAsFailed(futuresCopy, e);
            return;
        }
        futuresCopy.forEach((input, output) -> {
            output.setFuture(Futures.transform(
                    result,
                    map -> map.get(input),
                    MoreExecutors.directExecutor()));
        });
    }

    private void markAllAsFailed(Map<I, SettableFuture<O>> futures, Throwable thrown) {
        futures.values().forEach(future -> future.setException(thrown));
    }
}
