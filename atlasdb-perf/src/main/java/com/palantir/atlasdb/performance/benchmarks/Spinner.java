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

package com.palantir.atlasdb.performance.benchmarks;

import java.io.Closeable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import org.immutables.value.Value;

import com.google.common.util.concurrent.SettableFuture;
import com.palantir.common.concurrent.PTExecutors;

import jersey.repackaged.com.google.common.base.Throwables;

public final class Spinner<T> implements Supplier<T>, Closeable {
    private final ExecutorService executor = PTExecutors.newSingleThreadExecutor();
    private final ExecutorService publishingExecutor = PTExecutors.newSingleThreadExecutor();

    private volatile boolean closed = false;
    private volatile State<T> state = State.newState();

    public Spinner(Supplier<T> delegate) {
        executor.submit(() -> {
            while (!closed) {
                State<T> snapshot = state;
                try {
                    T result = delegate.get();
                    publishingExecutor.submit(() -> snapshot.present().set(result));
                } catch (Throwable t) {
                    publishingExecutor.submit(() -> snapshot.present().setException(t));
                }
                state = snapshot.toNext();
            }
        });
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public T get() {
        try {
            return state.next().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    private static final class Ref<T> {
        private volatile T ref;
    }

    @Value.Immutable
    interface State<T> {
        @Value.Parameter SettableFuture<T> present();
        @Value.Parameter SettableFuture<T> next();

        default State<T> toNext() {
            return ImmutableState.of(next(), SettableFuture.create());
        }

        static <T> State<T> newState() {
            return ImmutableState.of(SettableFuture.create(), SettableFuture.create());
        }
    }
}
