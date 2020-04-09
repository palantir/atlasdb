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

package com.palantir.atlasdb.v2.api.promise;

import static com.palantir.logsafe.Preconditions.checkNotNull;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.Executor;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;

public abstract class AbstractPromise<T> implements Promise<T> {
    private final Deque<Runnable> listeners = new ArrayDeque<>(1);

    private T result;
    private Throwable thrown;

    @Override
    public boolean isDone() {
        return result != null || thrown != null;
    }

    @Override
    public T get() {
        if (thrown != null) {
            throw new UncheckedExecutionException(thrown);
        } else if (result == null) {
            throw new IllegalStateException("Can't access an incomplete promise");
        } else {
            return result;
        }
    }

    protected boolean set(T result) {
        checkNotNull(result);
        if (isDone()) {
            return false;
        }
        this.result = result;
        triggerListeners();
        return true;
    }

    protected boolean setException(Throwable exception) {
        checkNotNull(exception);
        if (isDone()) {
            return false;
        }
        this.thrown = exception;
        triggerListeners();
        return true;
    }

    public void addListener(Runnable runnable, Executor executor) {
        if (isDone()) {
            executor.execute(runnable);
        } else {
            listeners.add(() -> executor.execute(runnable));
            listeners.forEach(AbstractPromise::runListener);
            listeners.clear();
        }
    }

    private void triggerListeners() {
        while (!listeners.isEmpty()) {
            runListener(listeners.remove());
        }
    }

    private static void runListener(Runnable listener) {
        try {
            listener.run();
        } catch (Throwable t) {
            // do nothing.
        }
    }

    @Override
    public ListenableFuture<T> toListenableFuture() {
        SettableFuture<T> future = SettableFuture.create();
        addListener(() -> {
            if (thrown != null) {
                future.setException(thrown);
            } else {
                future.set(result);
            }
        }, MoreExecutors.directExecutor());
        return null;
    }
}
