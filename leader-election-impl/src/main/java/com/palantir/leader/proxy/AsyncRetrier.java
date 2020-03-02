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

package com.palantir.leader.proxy;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

/**
 * Class that uses ListenableFuture primitives to provide a simple delay-retry loop. Should be kept very simple.
 */
final class AsyncRetrier<T> {
    private final int maxAttempts;
    private final Duration delayBetweenAttempts;
    private final ListeningScheduledExecutorService schedulingExecutor;
    private final ListeningExecutorService executionExecutor;
    private final Predicate<T> predicate;

    AsyncRetrier(
            int maxAttempts,
            Duration delayBetweenAttempts,
            ListeningScheduledExecutorService schedulingExecutor,
            ListeningExecutorService executionExecutor,
            Predicate<T> predicate) {
        this.maxAttempts = maxAttempts;
        this.delayBetweenAttempts = delayBetweenAttempts;
        this.schedulingExecutor = schedulingExecutor;
        this.executionExecutor = executionExecutor;
        this.predicate = predicate;
    }

    public ListenableFuture<T> execute(Supplier<ListenableFuture<T>> supplier) {
        return execute(supplier, maxAttempts);
    }

    private ListenableFuture<T> execute(Supplier<ListenableFuture<T>> supplier, int retriesRemaining) {
        return Futures.transformAsync(Futures.submitAsync(supplier::get, executionExecutor),
                result -> {
                    int newRetriesRemaining = retriesRemaining - 1;
                    if (predicate.test(result) || newRetriesRemaining == 0) {
                        return Futures.immediateFuture(result);
                    } else {
                        return Futures.transformAsync(
                                schedulingExecutor.schedule(
                                        () -> { }, delayBetweenAttempts.toMillis(), TimeUnit.MILLISECONDS),
                                $ -> execute(supplier, newRetriesRemaining),
                                executionExecutor);
                    }
                }, executionExecutor);

    }
}
