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

/**
 * Transforms a {@code Supplier} by an output function, possibly to a different type. This {@code Supplier} caches
 * results of applying the output function to the last seen input. This is thus usually appropriate where the value
 * of the input does not change excessively frequently, and where the computation of the output function is expensive.
 *
 * See also {@link CachedComposedSupplier}. However, that differs in that that {@code Supplier} implements periodic
 * cache invalidation, and also supports explicit versioning (in particular, for use cases where {@code equals()}
 * comparison on the input objects is also expensive).
 */
public class CachedTransformingSupplier<T, U> implements Supplier<U> {
    private final Supplier<T> inputSupplier;
    private final Function<T, U> outputFunction;

    private T cachedInput = null;
    private U cachedOutput = null;

    public CachedTransformingSupplier(Supplier<T> inputSupplier, Function<T, U> outputFunction) {
        this.inputSupplier = inputSupplier;
        this.outputFunction = outputFunction;
    }

    @Override
    public synchronized U get() {
        T currentInput = inputSupplier.get();

        if (Objects.equals(cachedInput, currentInput)) {
            return cachedOutput;
        }

        cachedOutput = outputFunction.apply(cachedInput);
        cachedInput = currentInput;
        return cachedOutput;
    }
}
