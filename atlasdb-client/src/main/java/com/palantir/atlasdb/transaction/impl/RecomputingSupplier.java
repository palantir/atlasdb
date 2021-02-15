/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.transaction.impl;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public final class RecomputingSupplier<T> implements Supplier<T> {
    final Supplier<? extends T> supplier;
    final AtomicReference<T> cache = new AtomicReference<>();
    final AtomicReference<CountDownLatch> latch = new AtomicReference<>();

    private RecomputingSupplier(Supplier<? extends T> supplier) {
        this.supplier = supplier;
    }

    @Override
    public T get() {
        T ret = cache.get();
        return ret == null ? recompute() : ret;
    }

    public T recompute() {
        CountDownLatch newLatch = new CountDownLatch(1);
        if (latch.compareAndSet(null, newLatch)) {
            try {
                cache.set(supplier.get());
            } finally {
                latch.set(null);
                newLatch.countDown();
            }
        } else {
            CountDownLatch currentLatch = latch.get();
            if (currentLatch != null) {
                try {
                    currentLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        return cache.get();
    }

    public static <T> RecomputingSupplier<T> create(Supplier<? extends T> supplier) {
        return new RecomputingSupplier<>(supplier);
    }
}
