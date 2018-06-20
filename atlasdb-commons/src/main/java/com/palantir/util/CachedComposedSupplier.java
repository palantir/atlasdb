/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.util;

import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A version of composed supplier that caches the result of applying the function to the value supplied by the
 * underlying supplier of {@link VersionedType}. The result is recomputed if and only if the returned version
 * increased since last call to get().
 *
 * Intended to be used when applying the function is expensive and the versioned type is expected to be updated
 * less frequently than calls to {@link CachedComposedSupplier#get()}.
 */
public class CachedComposedSupplier<T, R> implements Supplier<R> {
    private final Function<T, R> function;
    private final Supplier<VersionedType<T>> supplier;
    private volatile Long lastSuppliedVersion = null;
    private R cached;

    public CachedComposedSupplier(Function<T, R> function, Supplier<VersionedType<T>> supplier) {
        this.function = function;
        this.supplier = supplier;
    }

    @Override
    public R get() {
        if (!Objects.equals(supplier.get().version(), lastSuppliedVersion)) {
            recompute();
        }
        return cached;
    }

    private synchronized void recompute() {
        VersionedType<T> freshVersion = supplier.get();
        if (!Objects.equals(freshVersion.version(), lastSuppliedVersion)) {
            cached = function.apply(freshVersion.value());
            lastSuppliedVersion = freshVersion.version();
        }
    }
}
