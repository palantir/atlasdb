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

package com.palantir.atlasdb.keyvalue.cassandra.limiter;

import java.io.Closeable;
import java.util.concurrent.Semaphore;
import java.util.function.Supplier;

public final class SafeSemaphore<E extends Exception> {
    private final Semaphore delegate;
    private final Supplier<E> exception;

    private SafeSemaphore(Semaphore delegate, Supplier<E> exception) {
        this.delegate = delegate;
        this.exception = exception;
    }

    public CloseablePermit acquireOrThrow() throws E {
        if (!delegate.tryAcquire()) {
            throw exception.get();
        }
        return delegate::release;
    }

    public int availablePermits() {
        return delegate.availablePermits();
    }

    public static <E extends Exception> SafeSemaphore<E> of(int permits, Supplier<E> exception) {
        return new SafeSemaphore<>(new Semaphore(permits), exception);
    }

    public interface CloseablePermit extends Closeable {}
}
