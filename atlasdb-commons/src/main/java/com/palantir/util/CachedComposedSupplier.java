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
package com.palantir.util;

import com.palantir.common.time.Clock;
import com.palantir.common.time.SystemClock;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A version of composed supplier that caches the result of applying the function to the value supplied by the
 * underlying supplier of {@link VersionedType}. The result is recomputed if and only if the returned version
 * increased since last call to get(), or a specified amount of time has elapsed since the returned version has changed.
 *
 * Intended to be used when applying the function is expensive and the versioned type is expected to be updated
 * less frequently than calls to {@link CachedComposedSupplier#get()}.
 */
public class CachedComposedSupplier<T, R> implements Supplier<R> {
    private final Function<T, R> function;
    private final Supplier<VersionedType<T>> supplier;
    private final long maxTimeBetweenRecomputes;
    private final Clock clock;
    private volatile Long lastSuppliedVersion = null;
    private volatile long lastComputedTime = 0;
    private R cached;

    public CachedComposedSupplier(Function<T, R> function, Supplier<VersionedType<T>> supplier) {
        this(function, supplier, TimeUnit.MINUTES.toMillis(5), new SystemClock());
    }

    public CachedComposedSupplier(
            Function<T, R> function, Supplier<VersionedType<T>> supplier, long maxTimeBetweenRecomputes, Clock clock) {
        this.function = function;
        this.supplier = supplier;
        this.maxTimeBetweenRecomputes = maxTimeBetweenRecomputes;
        this.clock = clock;
    }

    @Override
    public R get() {
        if (!Objects.equals(supplier.get().version(), lastSuppliedVersion)
                || lastComputedTime + maxTimeBetweenRecomputes < clock.getTimeMillis()) {
            recompute();
        }
        return cached;
    }

    private synchronized void recompute() {
        VersionedType<T> freshVersion = supplier.get();
        if (!Objects.equals(freshVersion.version(), lastSuppliedVersion)
                || lastComputedTime + maxTimeBetweenRecomputes < clock.getTimeMillis()) {
            cached = function.apply(freshVersion.value());
            lastSuppliedVersion = freshVersion.version();
        }
        lastComputedTime = clock.getTimeMillis();
    }
}
