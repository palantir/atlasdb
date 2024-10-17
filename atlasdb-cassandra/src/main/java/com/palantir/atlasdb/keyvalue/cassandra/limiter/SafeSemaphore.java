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
import java.util.Optional;
import java.util.concurrent.Semaphore;
import javax.annotation.CheckReturnValue;

public final class SafeSemaphore {
    private final Semaphore delegate;

    private SafeSemaphore(Semaphore delegate) {
        this.delegate = delegate;
    }

    @CheckReturnValue
    public Optional<CloseablePermit> tryAcquire() {
        if (!delegate.tryAcquire()) {
            return Optional.empty();
        }

        return Optional.of(delegate::release);
    }

    public int availablePermits() {
        return delegate.availablePermits();
    }

    public static SafeSemaphore of(int permits) {
        return new SafeSemaphore(new Semaphore(permits));
    }

    public interface CloseablePermit extends Closeable {}
}
