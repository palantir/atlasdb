/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra.pool;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Futures;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Supplier that will evaluate a delegate supplier asynchronously, providing a result when the underlying computation
 * is complete. This computation is only initiated after the first call to get, but will be memoized infinitely after
 * that. This class is responsible for creating and shutting down any executors.
 *
 * This class is thread safe by synchronising on {@link #get()}.
 */
@ThreadSafe
final class AsyncSupplier<T> implements Supplier<Optional<T>> {
    private static final SafeLogger log = SafeLoggerFactory.get(AsyncSupplier.class);
    private final Supplier<Optional<T>> delegate;
    private final ExecutorService executorService;
    private Future<Optional<T>> result;

    @VisibleForTesting
    AsyncSupplier(Supplier<Optional<T>> delegate, ExecutorService executorService) {
        this.delegate = delegate;
        this.executorService = executorService;
    }

    @Override
    public synchronized Optional<T> get() {
        if (result == null) {
            result = executorService.submit(delegate::get);
            executorService.shutdown();
        } else if (result.isDone()) {
            try {
                return result.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SafeRuntimeException("Interrupted while attempting to compute supplier value", e);
            } catch (ExecutionException e) {
                log.warn("Failed to evaluate delegate supplier asynchronously; returning empty", e);
                result = Futures.immediateFuture(Optional.empty());
            }
        }
        return Optional.empty();
    }

    static <T> AsyncSupplier<T> create(Supplier<Optional<T>> delegate) {
        return new AsyncSupplier<>(delegate, PTExecutors.newSingleThreadExecutor());
    }
}
