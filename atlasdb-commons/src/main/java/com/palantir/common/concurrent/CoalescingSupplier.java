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
package com.palantir.common.concurrent;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.palantir.atlasdb.tracing.CloseableTrace;
import com.palantir.logsafe.SafeArg;
import com.palantir.tracing.Tracer;

/**
 * A supplier that coalesces computation requests, such that only one computation is ever running at a time, and
 * concurrent requests will result in a single computation. Computations are guaranteed to execute after being
 * requested; requests will not receive results for computations that started prior to the request.
 */
public class CoalescingSupplier<T> implements Supplier<T> {

    private static final Logger log = LoggerFactory.getLogger(CoalescingSupplier.class);

    private final Supplier<T> delegate;
    private final Optional<String> operation;
    private volatile CompletableFuture<T> nextResult = new CompletableFuture<>();

    private final Lock fairLock = new ReentrantLock(true);

    public CoalescingSupplier(Supplier<T> delegate) {
        this.delegate = delegate;
        this.operation = Optional.empty();
    }

    public CoalescingSupplier(Supplier<T> delegate, String operation) {
        this.delegate = delegate;
        this.operation = Optional.of(operation);
    }

    @Override
    public T get() {
        CompletableFuture<T> future = nextResult;

        completeOrWaitForCompletion(future);

        return getResult(future);
    }

    private void completeOrWaitForCompletion(CompletableFuture<T> future) {
        fairLock.lock();
        try {
            resetAndCompleteIfNotCompleted(future);
        } finally {
            fairLock.unlock();
        }
    }

    private CloseableTrace startLocalTrace(String operation) {
        return CloseableTrace.startLocalTrace("AtlasDB:CoalescingSupplier", operation + " {}", this.operation);
    }

    private void resetAndCompleteIfNotCompleted(CompletableFuture<T> future) {
        if (future.isDone()) {
            return;
        }

        nextResult = new CompletableFuture<T>();
        try (CloseableTrace ignored = startLocalTrace("executeDelegate" + operation)) {
            if (operation.isPresent()) {
                log.info("executing delegate for operation " + operation.get(),
                        SafeArg.of("operation", operation.get()),
                        SafeArg.of("thread", Thread.currentThread().getId()),
                        SafeArg.of("traceId", Tracer.getTraceId()));
            }

            T value = delegate.get();
            if (operation.isPresent()) {
                log.info("finished executing delegate for operation " + operation.get(),
                        SafeArg.of("operation", operation.get()),
                        SafeArg.of("thread", Thread.currentThread().getId()),
                        SafeArg.of("traceId", Tracer.getTraceId()));
            }
            future.complete(value);

        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
    }

    private T getResult(CompletableFuture<T> future) {
        try (CloseableTrace ignored = startLocalTrace("get result" + operation)) {
            if (operation.isPresent()) {
                log.info("retrieving from future for operation " + operation.get(),
                        SafeArg.of("operation", operation.get()),
                        SafeArg.of("thread", Thread.currentThread().getId()),
                        SafeArg.of("traceId", Tracer.getTraceId()));
            }
            T now = future.getNow(null);
            if (operation.isPresent()) {
                log.info("finished retrieving from future for operation " + operation.get(),
                        SafeArg.of("operation", operation.get()),
                        SafeArg.of("thread", Thread.currentThread().getId()),
                        SafeArg.of("traceId", Tracer.getTraceId()));
            }
            return now;
        } catch (CompletionException ex) {
            throw Throwables.propagate(ex.getCause());
        }
    }

}
