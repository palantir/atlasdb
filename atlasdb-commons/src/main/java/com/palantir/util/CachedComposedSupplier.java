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
 * underlying supplier. The result is recomputed only if the supplied value changes.
 *
 * Intended to be used when applying the function is expensive in comparison to getting the value from the underlying
 * supplier.
 */
public class CachedComposedSupplier<T> implements Supplier<T> {
    private final Function<Long, T> function;
    private final Supplier<VersionedLong> supplier;
    private volatile Long lastSuppliedVersion = null;
    private volatile T cached;

    public CachedComposedSupplier(Function<Long, T> function, Supplier<VersionedLong> supplier) {
        this.function = function;
        this.supplier = supplier;
    }

    @Override
    public T get() {
        if (!Objects.equals(supplier.get().version(), lastSuppliedVersion)) {
            recompute();
        }
        return cached;
    }

    private synchronized void recompute() {
        VersionedLong freshVersion = supplier.get();
        if (!Objects.equals(freshVersion.version(), lastSuppliedVersion)) {
            lastSuppliedVersion = freshVersion.version();
            cached = function.apply(freshVersion.value());
        }
    }
}
