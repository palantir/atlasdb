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

package com.palantir.atlasdb.sweep.queue;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

public class LazyRecomputingSupplier<T> implements Supplier<T> {
    private final Supplier<T> cheapSupplier;
    private final UnaryOperator<T> recomputingMethod;

    private AtomicReference<T> lastKnownFirst = new AtomicReference<>();
    private AtomicReference<T> cachedValue = new AtomicReference<>();

    public LazyRecomputingSupplier(Supplier<T> supplier, UnaryOperator<T> recomputingMethod) {
        this.cheapSupplier = supplier;
        this.recomputingMethod = recomputingMethod;
    }

    @Override
    public T get() {
        T lastKnown = lastKnownFirst.get();
        synchronized (this) {
            if (lastKnown == null || !lastKnown.equals(cheapSupplier.get())) {
                recompute();
            }
        }
        return cachedValue.get();
    }

    private void recompute() {
        T newFirst = cheapSupplier.get();
        lastKnownFirst.set(newFirst);
        cachedValue.set(recomputingMethod.apply(newFirst));
    }
}
