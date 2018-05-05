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

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * Supplier that lazily computes and caches the result of applying the given recomputingOperation to the return value
 * of the delegate Supplier. The recomputingOperation will be invoked each time (and only when) get() is called and
 * the return value of the delegate supplier has changed since the last invocation.
 */
public class LazyRecomputingSupplier<T> implements Supplier<T> {
    private final Supplier<T> delegate;
    private final UnaryOperator<T> recomputingOperation;

    private final AtomicReference<T> lastKnownDelegateValue = new AtomicReference<>();
    private final AtomicReference<T> cachedRecomputedValue = new AtomicReference<>();

    public LazyRecomputingSupplier(Supplier<T> delegate, UnaryOperator<T> recomputingOperation) {
        this.delegate = delegate;
        this.recomputingOperation = recomputingOperation;
    }

    @Override
    public T get() {
        T lastKnown = lastKnownDelegateValue.get();
        T current = delegate.get();
        synchronized (this) {
            if (lastKnown == null || !lastKnown.equals(current)) {
                recomputeValue(current);
            }
        }
        return cachedRecomputedValue.get();
    }

    private void recomputeValue(T current) {
        lastKnownDelegateValue.set(current);
        cachedRecomputedValue.set(recomputingOperation.apply(current));
    }
}
