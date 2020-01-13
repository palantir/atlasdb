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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Supplier;

import com.google.common.base.Throwables;

/**
 * A supplier that coalesces computation requests, such that only one computation is ever running at a time, and
 * concurrent requests will result in a single computation. Computations are guaranteed to execute after being
 * requested; requests will not receive results for computations that started prior to the request.
 */
public class CoalescingSupplier<T> implements Supplier<T> {
    private final Supplier<T> delegate;
    private volatile Round<T> nextResult = new Round<>(this);

    public CoalescingSupplier(Supplier<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public T get() {
        Round<T> present = nextResult;
        if (present.isFirstToArrive()) {
            present.execute();
            return present.getResult();
        }
        awaitDone(present.future);
        Round<T> next = present.next;
        if (next.isFirstToArrive()) {
            next.execute();
        }
        return next.getResult();
    }

    private void awaitDone(CompletableFuture<?> future) {
        try {
            future.join();
        } catch (CompletionException e) {
            // ignore
        }
    }

    private static final class Round<T> {
        private static final int TRUE = 1;
        private static final int FALSE = 0;
        private static final AtomicIntegerFieldUpdater<Round> hasStartedUpdater =
                AtomicIntegerFieldUpdater.newUpdater(Round.class, "hasStarted");
        private volatile int hasStarted = FALSE;
        private final CoalescingSupplier<T> supplier;
        private final CompletableFuture<T> future = new CompletableFuture<>();
        private volatile Round<T> next;

        private Round(CoalescingSupplier<T> supplier) {
            this.supplier = supplier;
        }

        boolean isFirstToArrive() {
            return hasStarted == FALSE && hasStartedUpdater.compareAndSet(this, FALSE, TRUE);
        }

        void execute() {
            next = new Round<>(supplier);
            supplier.nextResult = next;
            try {
                future.complete(supplier.delegate.get());
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        }

        T getResult() {
            try {
                return future.join();
            } catch (CompletionException e) {
                throw Throwables.propagate(e.getCause());
            }
        }
    }
}
