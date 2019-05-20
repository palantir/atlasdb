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

package com.palantir.util;

import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

public class MemoizedComposedSupplier<T, R> implements Supplier<R> {
    private final Function<T, R> function;
    private final Supplier<T> supplier;

    private volatile T lastKey;
    private R cached;

    public MemoizedComposedSupplier(Supplier<T> supplier, Function<T, R> function) {
        this.function = function;
        this.supplier = supplier;
    }

    public R get() {
        if (!Objects.equals(lastKey, supplier.get())) {
            recompute();
        }
        return cached;
    }

    private synchronized void recompute() {
        T freshKey = supplier.get();
        if (!Objects.equals(lastKey, freshKey)) {
            cached = function.apply(lastKey);
            lastKey = freshKey;
        }
    }
}
