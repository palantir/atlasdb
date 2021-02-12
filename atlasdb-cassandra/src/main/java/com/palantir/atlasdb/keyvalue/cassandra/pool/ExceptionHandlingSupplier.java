/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * A supplier wrapper that stops calling the delegate supplier after failing some number of times in a row.
 */
final class ExceptionHandlingSupplier<T> implements Supplier<Optional<T>> {
    private final Supplier<T> delegate;
    private final int numRetries;
    private final AtomicInteger numFailures;

    private ExceptionHandlingSupplier(Supplier<T> delegate, int numRetries) {
        this.delegate = delegate;
        this.numRetries = numRetries;
        numFailures = new AtomicInteger(0);
    }

    public static <T> ExceptionHandlingSupplier<T> create(Supplier<T> delegate, int numRetries) {
        return new ExceptionHandlingSupplier<>(delegate, numRetries);
    }

    @Override
    public Optional<T> get() {
        if (numFailures.get() > numRetries) {
            return Optional.empty();
        }

        try {
            T value = delegate.get();
            numFailures.set(0);
            return Optional.of(value);
        } catch (Throwable t) {
            numFailures.incrementAndGet();
            return Optional.empty();
        }
    }
}
