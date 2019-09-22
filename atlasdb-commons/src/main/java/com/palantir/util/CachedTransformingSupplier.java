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
 * WITHOUT WARRANTIES OU CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.util;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.collect.Maps;

public final class CachedTransformingSupplier<T, U> implements Supplier<U> {
    private final Supplier<T> inputSupplier;
    private final Function<T, U> outputFunction;

    private volatile Map.Entry<T, U> cache;

    public CachedTransformingSupplier(Supplier<T> inputSupplier, Function<T, U> outputFunction) {
        this.inputSupplier = inputSupplier;
        this.outputFunction = outputFunction;
    }

    @Override
    public U get() {
        T currentInput = inputSupplier.get();
        Map.Entry<T, U> snapshot = cache;
        if (snapshot != null && Objects.equals(snapshot.getKey(), currentInput)) {
            return snapshot.getValue();
        }
        synchronized (this) {
            if (snapshot != null && Objects.equals(snapshot.getKey(), currentInput)) {
                return snapshot.getValue();
            }
            cache = Maps.immutableEntry(currentInput, outputFunction.apply(currentInput));
            return cache.getValue();
        }
    }
}
